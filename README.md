# Store Chunky Bytes

Current library is a building block for content addressable persistence. Provides ability to persist, modify and retrieve large byte-arrays or fragments thereof. This is work in progress, features are still added, contributions welcome.

Current features:

1. __Persist__ large byte arrays, across multiple chunks, in the blob store of your choice (memory, cloud, ipfs, etc.) 
2. __Modify__ the persisted byte arrays, O(n) efficiency where n is the number of impacted chunks
3. __Retrieve__ slices of data, based on the offset, independent of the individual chunk boundaries 

Persisted data is content addressable, hence immutable and versionable.

Used in conjunction with [content defined chunkers](https://www.npmjs.com/package/@dstanesc/wasm-chunking-webpack-eval) (eg. Fastcdc and Buzhash) offers chunk deduplication across versions of data.

The intended usage is to persist and access collections of fixed size records. In this case the records can be retrieved extremely efficient (O1) based on the offsets computed externally using mathematic formulas rather than scanning the data.

## Change Log

- `v0.0.2` - API addition. Append data w/o retrieving the original byte array. I/O efficiency - O(n), n - number of new chunks
- `v0.0.3` - Breaking change. Introduce support to upcoming features by restructuring the index persisted structure - reducing size and moving from absolute to relative offsets 
- `v0.0.4` - API addition. Update data w/o retrieving the original byte array. I/O efficiency - O(n), n - number of chunks impacted by update
- `v0.0.5` - Bugfixes
- `v0.0.6` - Update enhancements. Improved heuristically chunk offset stability
- `v0.0.7` - API addition. Remove fragments of data w/o retrieving the original byte array.  I/O efficiency - O(n), n - number of chunks impacted by removal
- `v0.0.8` - API addition. Convenience function to read all blocks, ie. full byte array
- `v0.0.9` - Bugfix. Tolerate empty appends, also combined w/ empty creations
- `v0.0.10`- API addition. Bulk to combine append and update in a single roundtrip
- `v0.0.11`- Bugfix. Tolerate empty updates

## Usage

### Create & Retrieve

```js
import { chunkyStore } from '@dstanesc/store-chunky-bytes'
import { codec, blockStore, chunkerFactory } from './util'

const buf = ...

// configure utility functions
const { get, put } = blockStore()
const { encode, decode } = codec()
const { fastcdc, buzhash } = chunkerFactory({ fastAvgSize: 1024 * 16, buzHash: 15 })

// chunky store functionality
const { create, read } = chunkyStore()

// create blocks and store them in the block store
// the root is the cryptographic handle to the logical buffer and needs preserved for later access
const { root, blocks } = await create({ buf, chunk, encode })
blocks.forEach(block => put(block))

const startOffset = ...
const sliceLength = ...

// extract any slice of data 
const recordBytes = await read(startOffset, sliceLength, { root, decode, get })

// read all blocks, ie full byte array
const allBytes = await readAll({ root, decode, get })
```

For more details see the [store tests](https://github.com/dstanesc/store-chunky-bytes/blob/39b4ed9e6fa0af28bdad7f732c941fcf3b599a7a/src/__tests__/chunky-store.test.ts#L18-L50).

### Append

```js
const buf2 = ... // byte array to append

// append additional data
// same chunking algorithm as in the creation phase required
// mandatory content-defined chunking algorithm (eg. fastcdc. buzhash, etc.)
// returns a new root and the new blocks
const { root: appendRoot, blocks: appendBlocks } = await append({ root: origRoot, decode, get }, { buf: buf2, chunk: fastcdc, encode })
appendBlocks.forEach(block => put(block))

const startOffset = ...
const sliceLength = ...

// extract any slice of data from the combined byte arrays using the second root
const recordBytes = await read(startOffset, sliceLength, { root: appendRoot, decode, get })
```

For more details see the [append tests](https://github.com/dstanesc/store-chunky-bytes/blob/3f80f265ffed67df4d12cbcf8380ab19e9827050/src/__tests__/chunky-append.test.ts#L16)

### Update

```js
const buf2 = ... // byte array to replace original section

// update original data 
// same chunking algorithm as in the creation phase required
// mandatory content-defined chunking algorithm (eg. fastcdc. buzhash, etc.)
// returns a new root and the new blocks
const { root: updateRoot, index: updateIndex, blocks: updateBlocks } = await update({ root, decode, get }, { buf: buf2, chunk: fastcdc, encode }, RECORD_UPDATE_OFFSET)
updateBlocks.forEach(block => put(block))

```

> Note: Update alg. tuned heuristically for best stability (ie. compare chunk offsets after update w/ full chunking of the updated buffer) results w/ `fastcdc`. 

For more details see the [update tests](https://github.com/dstanesc/store-chunky-bytes/blob/2ced29bf28a55f0cd98a9434407414c09e0b795c/src/__tests__/chunky-delete.test.ts#L22)


### Bulk

```js
const buf2 = ... // append buffer
const buf3 = ... // update buffer
// combines append and update operations in this order
const { root: bulkRoot, index: bulkIndex, blocks: bulkBlocks } = await bulk({ root: origRoot, decode, get }, { appendBuffer: buf2, updateBuffer: buf3, chunk: fastcdc, encode }, RECORD_UPDATE_OFFSET)
```

### Remove

```js
const startOffset = ...
const sliceLength = ...

// delete a slice from original data 
// same chunking algorithm as in the creation phase required
// mandatory content-defined chunking algorithm (eg. fastcdc. buzhash, etc.)
// returns a new root and the new blocks
const { root: deleteRoot, index: deleteIndex, blocks: deleteBlocks } = await remove({ root, decode, get }, { chunk: fastcdc, encode }, startOffset, sliceLength)
deleteBlocks.forEach(block => put(block))
```

For more details see the [remove tests](https://github.com/dstanesc/store-chunky-bytes/blob/002b19771eebe7b573b0f8cc123d889d5a4413d2/src/__tests__/chunky-delete.test.ts#L22)

To keep library size, dependencies and flexibility under control the `blockStore`, the content identifier `encode/decode` and the `chunking` functionality are not part of the library. However, all batteries are included. The [test utilities](https://github.com/dstanesc/store-chunky-bytes/blob/main/src/__tests__/util.ts) offer basic functionality for reuse and extension.

## Build

```sh
npm run clean
npm install
npm run build
npm run test
```

## Licenses

Licensed under either [Apache](./LICENSE-APACHE) or [MIT](./LICENSE-MIT) at your option.
