import { v4 as uuidV4, parse as uuidParse, stringify as uuidStringify } from 'uuid';
import { codec, blockStore, chunkerFactory } from './util'
import { chunkyStore } from '../index'
import * as assert from 'assert';



describe("Read all chunks", function () {

    test("full extent read matches input buffer", async () => {
        const RECORD_COUNT = 2000
        const RECORD_SIZE_BYTES = 36

        // demo binary data
        const buf = new Uint8Array(RECORD_COUNT * RECORD_SIZE_BYTES)
        let cursor = 0
        const originalRecords = []
        for (let index = 0; index < RECORD_COUNT; index++) {
            const demoRecord = uuidV4();
            originalRecords.push(demoRecord)
            const bytes = uuidParse(demoRecord)
            buf.set(bytes, cursor * RECORD_SIZE_BYTES)
            cursor++
        }

        // configure chunky store
        const { get, put } = blockStore()
        const { encode, decode } = codec()
        const { create, readAll } = chunkyStore()
        const { fastcdc, buzhash } = chunkerFactory({ fastAvgSize: 1024 })

        // persist chunked binary data
        const { root, index, blocks } = await create({ buf, chunk: fastcdc, encode })
        blocks.forEach(block => put(block))

        console.log(`Buffer size ${buf.length}`)
        console.log(`Index byteArraySize ${index.indexStruct.byteArraySize}`)
        assert.equal(buf.length, index.indexStruct.byteArraySize)
        
        // extract full data, new readAll api
        const recordBytes = await readAll({ root, decode, get })

        // check matches input
        assert.deepEqual(recordBytes, buf)

    })

})
