#include <memory>
#include <sqlite3.h>

struct SqliteStore
{
	sqlite3* db = nullptr;
	sqlite3_stmt* stmtStoreKeyExists = nullptr;
	sqlite3_stmt* stmtStoreGet = nullptr;
	sqlite3_stmt* stmtStoreSet = nullptr;
	sqlite3_stmt* stmtMemoStoreGet = nullptr;
	sqlite3_stmt* stmtMemoStoreSet = nullptr;
	bool error = false;

	~SqliteStore()
	{
		if(stmtStoreKeyExists) sqlite3_finalize(stmtStoreKeyExists);
		if(stmtStoreGet) sqlite3_finalize(stmtStoreGet);
		if(stmtStoreSet) sqlite3_finalize(stmtStoreSet);
		if(stmtMemoStoreGet) sqlite3_finalize(stmtMemoStoreGet);
		if(stmtMemoStoreSet) sqlite3_finalize(stmtMemoStoreSet);
		if(db) sqlite3_close(db);
	}

	void setError()
	{
		error = true;
	}
};

extern "C" void const* messdb_sqlite_store_create_blob(void const* data, int size);

extern "C" SqliteStore* messdb_sqlite_store_open(char const* path)
{
	std::unique_ptr<SqliteStore> store = std::make_unique<SqliteStore>();

	// open db
	int e;
	if((e = sqlite3_open(path, &store->db)) != SQLITE_OK)
		return nullptr;

	// set settings
	if(sqlite3_exec(store->db, "\
PRAGMA locking_mode = EXCLUSIVE; \
PRAGMA journal_mode = WAL; \
PRAGMA synchronous = OFF; \
", nullptr, nullptr, nullptr) != SQLITE_OK)
		return nullptr;

	// create tables
	if(sqlite3_exec(store->db, "\
CREATE TABLE IF NOT EXISTS store \
	( key BLOB PRIMARY KEY \
	, value BLOB NOT NULL \
	); \
CREATE TABLE IF NOT EXISTS memo_store \
	( key BLOB PRIMARY KEY \
	, value BLOB NOT NULL \
	); \
", nullptr, nullptr, nullptr) != SQLITE_OK)
		return nullptr;

	// prepare statements
	if(sqlite3_prepare_v3(store->db,
		"SELECT 1 FROM store WHERE key = ?",
		-1, SQLITE_PREPARE_PERSISTENT, &store->stmtStoreKeyExists, nullptr) != SQLITE_OK)
		return nullptr;
	if(sqlite3_prepare_v3(store->db,
		"SELECT value FROM store WHERE key = ?",
		-1, SQLITE_PREPARE_PERSISTENT, &store->stmtStoreGet, nullptr) != SQLITE_OK)
		return nullptr;
	if(sqlite3_prepare_v3(store->db,
		"INSERT INTO store(key, value) VALUES(?1, ?2) ON CONFLICT(key) DO UPDATE SET value = ?2",
		-1, SQLITE_PREPARE_PERSISTENT, &store->stmtStoreSet, nullptr) != SQLITE_OK)
		return nullptr;
	if(sqlite3_prepare_v3(store->db,
		"SELECT value FROM memo_store WHERE key = ?",
		-1, SQLITE_PREPARE_PERSISTENT, &store->stmtMemoStoreGet, nullptr) != SQLITE_OK)
		return nullptr;
	if(sqlite3_prepare_v3(store->db,
		"INSERT INTO memo_store(key, value) VALUES(?1, ?2) ON CONFLICT(key) DO UPDATE SET value = ?2",
		-1, SQLITE_PREPARE_PERSISTENT, &store->stmtMemoStoreSet, nullptr) != SQLITE_OK)
		return nullptr;

	return store.release();
}

extern "C" void messdb_sqlite_store_close(SqliteStore* store)
{
	delete store;
}

extern "C" int messdb_sqlite_store_errored(SqliteStore* store)
{
	bool errored = store->error;
	store->error = false;
	return errored ? 1 : 0;
}

int storeKeyExists(SqliteStore* store, sqlite3_stmt* stmt, void const* key, int keySize)
{
	if(sqlite3_bind_blob(stmt, 1, key, keySize, nullptr) != SQLITE_OK)
	{
		store->setError();
		return false;
	}

	bool exists = 0;
	for(bool stop = false; !stop; )
	{
		switch(sqlite3_step(stmt))
		{
		case SQLITE_ROW:
			exists = 1;
			break;
		case SQLITE_DONE:
			stop = true;
			break;
		default:
			stop = true;
			store->setError();
			break;
		}
	}

	sqlite3_reset(stmt);
	sqlite3_clear_bindings(stmt);

	return exists;
}

void storeGet(SqliteStore* store, sqlite3_stmt* stmt, void const* key, int keySize, void const** const value, int* const valueSize)
{
	*value = nullptr;
	*valueSize = 0;

	if(sqlite3_bind_blob(stmt, 1, key, keySize, nullptr) != SQLITE_OK)
	{
		store->setError();
		return;
	}

	for(bool stop = false; !stop; )
	{
		switch(sqlite3_step(stmt))
		{
		case SQLITE_ROW:
			{
				void const* data = sqlite3_column_blob(stmt, 0);
				int size = sqlite3_column_bytes(stmt, 0);
				*value = messdb_sqlite_store_create_blob(data, size);
				*valueSize = size;
			}
			break;
		case SQLITE_DONE:
			stop = true;
			break;
		default:
			stop = true;
			store->setError();
			break;
		}
	}

	sqlite3_reset(stmt);
	sqlite3_clear_bindings(stmt);
}

void storeSet(SqliteStore* store, sqlite3_stmt* stmt, void const* key, int keySize, void const* value, int valueSize)
{
	if(sqlite3_bind_blob(stmt, 1, key, keySize, nullptr) != SQLITE_OK)
	{
		store->setError();
		return;
	}
	if(sqlite3_bind_blob(stmt, 2, value, valueSize, nullptr) != SQLITE_OK)
	{
		store->setError();
		return;
	}

	if(sqlite3_step(stmt) != SQLITE_DONE) store->setError();

	sqlite3_reset(stmt);
	sqlite3_clear_bindings(stmt);
}

extern "C" int messdb_sqlite_store_key_exists(SqliteStore* store, void const* key, int keySize)
{
	return storeKeyExists(store, store->stmtStoreKeyExists, key, keySize);
}

extern "C" void messdb_sqlite_store_get(SqliteStore* store, void const* key, int keySize, void const** const value, int* const valueSize)
{
	storeGet(store, store->stmtStoreGet, key, keySize, value, valueSize);
}

extern "C" void messdb_sqlite_store_set(SqliteStore* store, void const* key, int keySize, void const* value, int valueSize)
{
	storeSet(store, store->stmtStoreSet, key, keySize, value, valueSize);
}

extern "C" void messdb_sqlite_memo_store_get(SqliteStore* store, void const* key, int keySize, void const** const value, int* const valueSize)
{
	storeGet(store, store->stmtMemoStoreGet, key, keySize, value, valueSize);
}

extern "C" void messdb_sqlite_memo_store_set(SqliteStore* store, void const* key, int keySize, void const* value, int valueSize)
{
	storeSet(store, store->stmtMemoStoreSet, key, keySize, value, valueSize);
}
