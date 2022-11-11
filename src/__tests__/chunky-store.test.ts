
import { partReport } from '@dstanesc/fake-metrology-data'
import { simpleMaterialJson } from '@dstanesc/fake-material-data'
import { unpack, pack } from 'msgpackr'
import { sha256 } from 'multiformats/hashes/sha2'
import { v4 as uuidV4, parse as uuidParse, stringify as uuidStringify } from 'uuid';


import { codec, blockStore, chunkerFactory } from './util'
import { chunkyStore } from '../index'

import * as assert from 'assert';



describe("Chunky bytes", function () {

    test("persist / query usage pattern demonstrator", async () => {
        const RECORD_COUNT = 2000
        const RECORD_SIZE_BYTES = 36

        // demo binary data
        const buf = new Uint8Array(RECORD_COUNT * RECORD_SIZE_BYTES)
        let cursor = 0
        const originalRecords = []
        for (let index = 0; index < RECORD_COUNT; index++) {
            const demoRecord = uuidV4();
            originalRecords.push(demoRecord)
            //const bytes = new TextEncoder().encode(demoRecord)
            const bytes = uuidParse(demoRecord)
            buf.set(bytes, cursor * RECORD_SIZE_BYTES)
            cursor++
        }

        // configure chunky store
        const { get, put } = blockStore()
        const { encode, decode } = codec()
        const { create, read } = chunkyStore()
        const { fastcdc, buzhash } = chunkerFactory({ fastAvgSize: 512 })

        // persist chunked binary data
        const { root, index, blocks } = await create({ buf, chunk: fastcdc, encode })
        for (const block of blocks) await put(block)
        console.log(blocks.length)
        // extract a slice of the chunked data independent of chunk boundaries (eg. record 999) 
        const recordBytes = await read(999 * RECORD_SIZE_BYTES, RECORD_SIZE_BYTES, { root, decode, get })

        // decode binary data into business domain
        const recordFound = uuidStringify(recordBytes)
        //const recordFound = new TextDecoder().decode(recordBytes)
        assert.equal(recordFound, originalRecords[999])
    })

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
            //const bytes = new TextEncoder().encode(demoRecord)
            buf.set(bytes, cursor * RECORD_SIZE_BYTES)
            cursor++
        }

        // configure chunky store
        const { get, put } = blockStore()
        const { encode, decode } = codec()
        const { create, read } = chunkyStore()
        const { fastcdc, buzhash } = chunkerFactory({ fastAvgSize: 1024 })

        // persist chunked binary data
        const { root, index, blocks } = await create({ buf, chunk: fastcdc, encode })
        for (const block of blocks) await put(block)

        // extract full data
        const recordBytes = await read(0, RECORD_SIZE_BYTES * 2000, { root, decode, get })

        // check matches input
        assert.deepEqual(recordBytes, buf)

    })

    test("full extent read matches input data", async () => {
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
            //const bytes = new TextEncoder().encode(demoRecord)
            buf.set(bytes, cursor * RECORD_SIZE_BYTES)
            cursor++
        }

        // configure chunky store
        const { get, put } = blockStore()
        const { encode, decode } = codec()
        const { create, read } = chunkyStore()
        const { fastcdc, buzhash } = chunkerFactory({})

        // persist chunked binary data
        const { root, index, blocks } = await create({ buf, chunk: fastcdc, encode })
        for (const block of blocks) await put(block)

        const completeBuffer = await read(0, RECORD_SIZE_BYTES * RECORD_COUNT, { root, decode, get })
        let cursor2 = 0
        const retrievedRecords = []
        for (let index = 0; index < RECORD_COUNT; index++) {
            const recordBytes = completeBuffer.subarray(cursor2 * RECORD_SIZE_BYTES, cursor2 * RECORD_SIZE_BYTES + RECORD_SIZE_BYTES)
            const recordFound = uuidStringify(recordBytes)
            //const recordFound = new TextDecoder().decode(recordBytes)
            retrievedRecords.push(recordFound)
            cursor2++
        }

        // check originalRecords and retrievedRecords same
        let diff1 = originalRecords.filter(x => !retrievedRecords.includes(x))
        let diff2 = retrievedRecords.filter(x => !originalRecords.includes(x))

        assert.equal(originalRecords.length, retrievedRecords.length)

        if (diff1.length > 0) throw new Error("Original and retrieved records should overlap")
        if (diff2.length > 0) throw new Error("Original and retrieved records should overlap")
    })

    test("blocks should be indexed properly", async () => {

        const reportData = partReport({ reportSize: 900 })
        const buf = pack(reportData)

        const { encode } = codec()
        const { create } = chunkyStore()
        const { fastcdc, buzhash } = chunkerFactory({})

        const { root, index, blocks } = await create({ buf, chunk: buzhash, encode })

        index.indexStruct.startOffsets.forEach((cid, offset) => console.log(`Offset ${offset} -> block ${cid.toString()}`))

        // same size, accounts for additional root block 
        assert.equal(index.indexStruct.indexSize, blocks.length - 1)
        // last block is the root
        assert.equal(root, blocks[blocks.length - 1].cid)
        // first cids match
        assert.equal(index.indexStruct.startOffsets.get(0), blocks[0].cid)
        // all index and block cids match
        const cids = Array.from(index.indexStruct.startOffsets.values())
        cids.forEach((val, index) => assert.equal(val, blocks[index].cid))
    })

    test("similar data should properly reuse blocks", async () => {

        const RECORD_COUNT = 200
        const firstSet = []
        for (let index = 0; index < RECORD_COUNT; index++) {
            firstSet.push(simpleMaterialJson())
        }
        const buf1: Uint8Array = pack(firstSet)

        // replace single material in the set
        const secondSet = [...firstSet]
        secondSet[1000] = simpleMaterialJson()
        const buf2: Uint8Array = pack(secondSet)

        const { encode } = codec()
        const { create } = chunkyStore()
        const { fastcdc, buzhash } = chunkerFactory({fastAvgSize: 1024 * 16})

        const { root: r1, blocks: b1 } = await create({ buf: buf1, chunk: fastcdc, encode })
        const { root: r2, blocks: b2 } = await create({ buf: buf2, chunk: fastcdc, encode })

        assert.notEqual(r1, r2)

        compareBlocks(b1, b2);

        const { root: r3, blocks: b3 } = await create({ buf: buf1, chunk: buzhash, encode })
        const { root: r4, blocks: b4 } = await create({ buf: buf2, chunk: buzhash, encode })

        assert.notEqual(r3, r4)

        compareBlocks(b3, b4);
    })

    test("persist / query by passing internal index reference rather than root", async () => {
        const RECORD_COUNT = 2000
        const RECORD_SIZE_BYTES = 36

        // demo binary data
        const buf = new Uint8Array(RECORD_COUNT * RECORD_SIZE_BYTES)
        let cursor = 0
        const originalRecords = []
        for (let index = 0; index < RECORD_COUNT; index++) {
            const demoRecord = uuidV4();
            originalRecords.push(demoRecord)
            //const bytes = new TextEncoder().encode(demoRecord)
            const bytes = uuidParse(demoRecord)
            buf.set(bytes, cursor * RECORD_SIZE_BYTES)
            cursor++
        }

        // configure chunky store
        const { get, put } = blockStore()
        const { encode, decode } = codec()
        const { create, read } = chunkyStore()
        const { fastcdc, buzhash } = chunkerFactory({ fastAvgSize: 512 })

        // persist chunked binary data
        const { root, index, blocks } = await create({ buf, chunk: fastcdc, encode })
        for (const block of blocks) await put(block)
        console.log(blocks.length)

        // extract a slice of the chunked by passing the index reference rather than root
        const recordBytes = await read(999 * RECORD_SIZE_BYTES, RECORD_SIZE_BYTES, { index, decode, get })

        // decode binary data into business domain
        const recordFound = uuidStringify(recordBytes)
        //const recordFound = new TextDecoder().decode(recordBytes)
        assert.equal(recordFound, originalRecords[999])
    })


})

function compareBlocks(b1: { cid: any; bytes: Uint8Array; }[], b2: { cid: any; bytes: Uint8Array; }[]) {
    
    const c1 = b1.map(block => block.cid.toString());
    const c2 = b2.map(block => block.cid.toString());

    let over = c2.filter(x => c1.includes(x));
    let diff = c2.filter(x => !c1.includes(x));

    console.log(`Total ${c2.length}`);
    console.log(`Overlap ${over.length}`);
    console.log(`New blocks ${diff.length}`);
    const percent = ((diff.length / c2.length) * 100);
    console.log(`Diff % ${percent}`);

    if (percent > 3)
        assert.fail("Should not create new blocks more than 5%");
}
