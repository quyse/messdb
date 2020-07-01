#include <memory>
#include <lmdb.h>

struct LmdbStore
{
	MDB_env* env = nullptr;
	MDB_dbi dbiStore = 0;
	MDB_dbi dbiMemoStore = 0;
	bool error = false;

	~LmdbStore()
	{
		// this closes everything
		if(env) mdb_env_close(env);
	}

	bool failed(int r)
	{
		if(r != 0)
		{
			error = true;
			return true;
		}
		return false;
	}
};

extern "C" void const* messdb_lmdb_store_create_blob(void const* data, int size);

extern "C" LmdbStore* messdb_lmdb_store_open(char const* path)
{
	std::unique_ptr<LmdbStore> store = std::make_unique<LmdbStore>();

	// create env
	if(store->failed(mdb_env_create(&store->env)))
		return nullptr;
	if(store->failed(mdb_env_set_mapsize(store->env, 0x400000000)))
		return nullptr;
	if(store->failed(mdb_env_set_maxdbs(store->env, 2)))
		return nullptr;
	if(store->failed(mdb_env_open(store->env, path, MDB_NOSUBDIR | MDB_WRITEMAP | MDB_NOSYNC | MDB_NOTLS, 0644)))
		return nullptr;

	{
		// open transaction for opening databases
		MDB_txn* txn = nullptr;
		if(store->failed(mdb_txn_begin(store->env, nullptr, 0, &txn)))
			return nullptr;

		// open store database
		if(store->failed(mdb_dbi_open(txn, "store", MDB_CREATE, &store->dbiStore)))
			return nullptr;
		// open memo store database
		if(store->failed(mdb_dbi_open(txn, "memo_store", MDB_CREATE, &store->dbiMemoStore)))
			return nullptr;

		// commit transaction
		if(store->failed(mdb_txn_commit(txn)))
			return nullptr;
	}

	return store.release();
}

extern "C" void messdb_lmdb_store_close(LmdbStore* store)
{
	delete store;
}

extern "C" int messdb_lmdb_store_errored(LmdbStore* store)
{
	bool errored = store->error;
	store->error = false;
	return errored ? 1 : 0;
}

int storeKeyExists(LmdbStore* store, MDB_dbi dbi, void const* key, size_t keySize)
{
	MDB_txn* txn = nullptr;
	if(store->failed(mdb_txn_begin(store->env, nullptr, MDB_RDONLY, &txn)))
		return 0;
	MDB_val valKey =
	{
		.mv_size = keySize,
		.mv_data = const_cast<void*>(key)
	};
	MDB_val valValue =
	{
		.mv_size = 0,
		.mv_data = nullptr
	};
	int r = mdb_get(txn, dbi, &valKey, &valValue);
	mdb_txn_abort(txn);
	switch(r)
	{
	case 0:
		return 1;
	case MDB_NOTFOUND:
		return 0;
	default:
		store->failed(r);
		return 0;
	}
}

void storeGet(LmdbStore* store, MDB_dbi dbi, void const* key, size_t keySize, void const** const value, size_t* const valueSize)
{
	*value = nullptr;
	*valueSize = 0;

	MDB_txn* txn = nullptr;
	if(store->failed(mdb_txn_begin(store->env, nullptr, MDB_RDONLY, &txn)))
		return;
	MDB_val valKey =
	{
		.mv_size = keySize,
		.mv_data = const_cast<void*>(key)
	};
	MDB_val valValue =
	{
		.mv_size = 0,
		.mv_data = nullptr
	};
	int r = mdb_get(txn, dbi, &valKey, &valValue);
	mdb_txn_abort(txn);
	switch(r)
	{
	case 0:
		*value = messdb_lmdb_store_create_blob(valValue.mv_data, valValue.mv_size);
		*valueSize = valValue.mv_size;
		break;
	case MDB_NOTFOUND:
		break;
	default:
		store->failed(r);
		break;
	}
}

void storeSet(LmdbStore* store, MDB_dbi dbi, void const* key, size_t keySize, void const* value, size_t valueSize)
{
	MDB_txn* txn = nullptr;
	if(store->failed(mdb_txn_begin(store->env, nullptr, 0, &txn)))
		return;
	MDB_val valKey =
	{
		.mv_size = keySize,
		.mv_data = const_cast<void*>(key)
	};
	MDB_val valValue =
	{
		.mv_size = valueSize,
		.mv_data = const_cast<void*>(value)
	};
	if(store->failed(mdb_put(txn, dbi, &valKey, &valValue, 0)))
	{
		mdb_txn_abort(txn);
		return;
	}

	if(store->failed(mdb_txn_commit(txn)))
		return;
}

extern "C" int messdb_lmdb_store_key_exists(LmdbStore* store, void const* key, int keySize)
{
	return storeKeyExists(store, store->dbiStore, key, keySize);
}

extern "C" void messdb_lmdb_store_get(LmdbStore* store, void const* key, size_t keySize, void const** const value, size_t* const valueSize)
{
	return storeGet(store, store->dbiStore, key, keySize, value, valueSize);
}

extern "C" void messdb_lmdb_store_set(LmdbStore* store, void const* key, size_t keySize, void const* value, size_t valueSize)
{
	return storeSet(store, store->dbiStore, key, keySize, value, valueSize);
}

extern "C" void messdb_lmdb_memo_store_get(LmdbStore* store, void const* key, size_t keySize, void const** const value, size_t* const valueSize)
{
	return storeGet(store, store->dbiMemoStore, key, keySize, value, valueSize);
}

extern "C" void messdb_lmdb_memo_store_set(LmdbStore* store, void const* key, size_t keySize, void const* value, size_t valueSize)
{
	return storeSet(store, store->dbiMemoStore, key, keySize, value, valueSize);
}
