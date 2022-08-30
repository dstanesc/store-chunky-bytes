# Store Chunky Bytes

Current library is a building block to :

1. __Persist__ large byte arrays, across multiple chunks, in the blob store of your choice (memory, cloud, ipfs, etc.) 
2. __Retrieve__ slices of data, based on the offset, independent of the individual chunk boundaries 

Persisted data is content addressable, hence immutable and versionable.

Used in conjunction with [content defined chunkers](https://www.npmjs.com/package/@dstanesc/wasm-chunking-webpack-eval)  (eg. Fastcdc and Buzhash) offers chunk deduplication across versions of data.

The intended usage is to persist and access collections of fixed size records. In this case the records can be retrieved extremely efficient (O1) based on the offsets computed externally using mathematic formulas rather than scanning the data.


## Usage

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

// extract a slice of the chunked data 
const recordBytes = await read(startOffset, sliceLength, { root, decode, get })

```

For more details see the [tests](https://github.com/dstanesc/store-chunky-bytes/blob/39b4ed9e6fa0af28bdad7f732c941fcf3b599a7a/src/__tests__/chunky-store.test.ts#L18-L50).

To keep library size, dependencies and flexibility under control the `blockStore`, the content identifier `encode/decode` and the `chunking` functionality are not part of the library. However, all batteries are included. The [test utilities](https://github.com/dstanesc/store-chunky-bytes/blob/main/src/__tests__/util.ts) offer basic functionality for reuse and extension.

## Build

```sh
npm run clean
npm install
npm run build
npm run test
```

## Licenses

Licensed under either of

* Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
* MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
