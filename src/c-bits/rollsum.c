#include <stdlib.h>
#include <stdint.h>
#include <memory.h>

#define WINDOW_SIZE 64
#define CHAR_OFFSET 31
#define SIGNAL_BITS 13
#define SIGNAL_OFFSET 3
#define SIGNAL_MASK (((1 << SIGNAL_BITS) - 1) << SIGNAL_OFFSET)

struct Rollsum
{
	uint32_t s1, s2;
	uint8_t w[WINDOW_SIZE];
	uint8_t p;
};

void rollsumInit(struct Rollsum* r)
{
	r->s1 = WINDOW_SIZE * CHAR_OFFSET;
	r->s2 = WINDOW_SIZE * (WINDOW_SIZE - 1) * CHAR_OFFSET;
	memset(r->w, 0, WINDOW_SIZE);
	r->p = 0;
}

uint32_t rollsumDigest(struct Rollsum* r)
{
	return (r->s1 << 16) | (r->s2 & 0xffff);
}

uint8_t rollsumSum(struct Rollsum* r, uint8_t const* buf, size_t len)
{
	uint8_t f = 0;
	for(size_t i = 0; i < len; ++i)
	{
		r->s1 += buf[i] - r->w[r->p];
		r->s2 += r->s1 - WINDOW_SIZE * (r->w[r->p] + CHAR_OFFSET);
		r->w[r->p] = buf[i];
		r->p = (r->p + 1) % WINDOW_SIZE;
		f |= (r->s2 & SIGNAL_MASK) == SIGNAL_MASK;
	}
	return f;
}
