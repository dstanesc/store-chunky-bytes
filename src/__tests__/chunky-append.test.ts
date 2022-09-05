import { unpack, pack } from 'msgpackr'
import { sha256 } from 'multiformats/hashes/sha2'
import { v4 as uuidV4, parse as uuidParse, stringify as uuidStringify } from 'uuid';


import { codec, blockStore, chunkerFactory } from './util'
import { chunkyStore } from '../index'

import * as assert from 'assert';


const RECORD_APPEND_COUNT = 100
const RECORD_COUNT = 2000
const RECORD_SIZE_BYTES = 36

describe("Chunky append", function () {


    test("append root maintains original root offsets", async () => {

        // configure chunky store
        const { get, put } = blockStore()
        const { encode, decode } = codec()
        const { create, read, append } = chunkyStore()
        const { fastcdc } = chunkerFactory({ fastAvgSize: 512 })

        // demo binary data
        const { buf, records: startRecords } = demoByteArray(RECORD_COUNT, RECORD_SIZE_BYTES)


        // persist chunked binary data
        const { root: origRoot, index: origIndex, blocks: origBlocks } = await create({ buf, chunk: fastcdc, encode })
        origBlocks.forEach(block => put(block))

        // demo binary data to append
        const { buf: buf2, records: appendedRecords } = demoByteArray(RECORD_APPEND_COUNT, RECORD_SIZE_BYTES)
        // append binary data
        const { root: appendRoot, index: appendIndex, blocks: appendBlocks } = await append({ root: origRoot, decode, get }, { buf: buf2, chunk: fastcdc, encode })
        appendBlocks.forEach(block => put(block))

        const origRecordBuffer = await read(1999 * RECORD_SIZE_BYTES, RECORD_SIZE_BYTES, { root: origRoot, decode, get })
        const origRecordFound = new TextDecoder().decode(origRecordBuffer)

        const appendRecordBuffer = await read(1999 * RECORD_SIZE_BYTES, RECORD_SIZE_BYTES, { root: appendRoot, decode, get })
        const appendRecordFound = new TextDecoder().decode(appendRecordBuffer)

        assert.equal(origRecordFound, appendRecordFound)

    })

    test("append and retrieve full data to existing chunky bytes, fastcdc", async () => {

        // demo binary data
        const { buf, records: startRecords } = demoByteArray(RECORD_COUNT, RECORD_SIZE_BYTES);

        // configure chunky store
        const { get, put } = blockStore()
        const { encode, decode } = codec()
        const { create, read, append } = chunkyStore()
        const { fastcdc } = chunkerFactory({ fastAvgSize: 512 })

        // persist chunked binary data
        const { root: origRoot, index: origIndex, blocks: origBlocks } = await create({ buf, chunk: fastcdc, encode })
        origBlocks.forEach(block => put(block))
        console.log(origIndex.startOffsets)

        // demo binary data to append
        const { buf: buf2, records: appendedRecords } = demoByteArray(RECORD_APPEND_COUNT, RECORD_SIZE_BYTES)
        // append binary data
        const { root: appendRoot, index: appendIndex, blocks: appendBlocks } = await append({ root: origRoot, decode, get }, { buf: buf2, chunk: fastcdc, encode })
        appendBlocks.forEach(block => put(block))
        console.log(appendIndex.startOffsets)

        const retrievedRecords1 = await retrieveRecords(read, 0, RECORD_COUNT, { root: origRoot, decode, get })
        assert.equal(startRecords.length, retrievedRecords1.length)
        assert.deepEqual(startRecords, retrievedRecords1)

        const retrievedRecords2 = await retrieveRecords(read, 0, RECORD_COUNT + RECORD_APPEND_COUNT, { root: appendRoot, decode, get })

        const recordUnion = startRecords.concat(appendedRecords)
        assert.equal(recordUnion.length, retrievedRecords2.length)
        assert.deepEqual(recordUnion, retrievedRecords2)
    })

    test("append and retrieve full data to existing chunky bytes, buzhash", async () => {

        // demo binary data
        const { buf, records: startRecords } = demoByteArray(RECORD_COUNT, RECORD_SIZE_BYTES);

        // configure chunky store
        const { get, put } = blockStore()
        const { encode, decode } = codec()
        const { create, read, append } = chunkyStore()
        const { buzhash } = chunkerFactory({ buzMask: 9 })

        // persist chunked binary data
        const { root: origRoot, index: origIndex, blocks: origBlocks } = await create({ buf, chunk: buzhash, encode })
        origBlocks.forEach(block => put(block))
        console.log(origIndex.startOffsets)

        // demo binary data to append
        const { buf: buf2, records: appendedRecords } = demoByteArray(RECORD_APPEND_COUNT, RECORD_SIZE_BYTES)
        // append binary data
        const { root: appendRoot, index: appendIndex, blocks: appendBlocks } = await append({ root: origRoot, decode, get }, { buf: buf2, chunk: buzhash, encode })
        appendBlocks.forEach(block => put(block))
        console.log(appendIndex.startOffsets)

        const retrievedRecords1 = await retrieveRecords(read, 0, RECORD_COUNT, { root: origRoot, decode, get })
        assert.equal(startRecords.length, retrievedRecords1.length)
        assert.deepEqual(startRecords, retrievedRecords1)

        const retrievedRecords2 = await retrieveRecords(read, 0, RECORD_COUNT + RECORD_APPEND_COUNT, { root: appendRoot, decode, get })

        const recordUnion = startRecords.concat(appendedRecords)
        assert.equal(recordUnion.length, retrievedRecords2.length)
        assert.deepEqual(recordUnion, retrievedRecords2)
    })
})

async function retrieveRecords(read: any, startOffset: number, recordCount: number, { root, decode, get }): Promise<any[]> {
    const completeBuffer = await read(startOffset, RECORD_SIZE_BYTES * recordCount, { root, decode, get })
    let cursor = 0
    const retrievedRecords = []
    for (let index = 0; index < recordCount; index++) {
        const recordBytes = completeBuffer.subarray(cursor * RECORD_SIZE_BYTES, cursor * RECORD_SIZE_BYTES + RECORD_SIZE_BYTES)
        const recordFound = new TextDecoder().decode(recordBytes)
        retrievedRecords.push(recordFound)
        cursor++
    }
    return retrievedRecords
}

function demoByteArray(recordCount: number, RECORD_SIZE_BYTES: number): { buf: Uint8Array, records: any[] } {
    const buf = new Uint8Array(recordCount * RECORD_SIZE_BYTES);
    let cursor = 0;
    const records = [];
    for (let index = 0; index < recordCount; index++) {
        const demoRecord = uuidV4();
        records.push(demoRecord);
        const bytes = new TextEncoder().encode(demoRecord);
        buf.set(bytes, cursor * RECORD_SIZE_BYTES);
        cursor++;
    }
    return { buf, records }
}

