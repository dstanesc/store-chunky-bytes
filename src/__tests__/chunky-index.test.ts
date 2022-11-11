import { unpack, pack } from 'msgpackr'
import { sha256 } from 'multiformats/hashes/sha2'
import { v4 as uuidV4, parse as uuidParse, stringify as uuidStringify, validate as uuidValidate, version as uuidVersion } from 'uuid';


import { codec, blockStore, chunkerFactory } from './util'
import { chunkyStore } from '../index'

import * as assert from 'assert';


const RECORD_COUNT = 100
const RECORD_APPEND_COUNT = 50
const RECORD_SIZE_BYTES = 36

describe("Chunky index", function () {


    test("create and readIndex generate identical index structure", async () => {

        // configure chunky store
        const { get, put } = blockStore()
        const { encode, decode } = codec()
        const { create, read, append, readIndex } = chunkyStore()
        const { fastcdc } = chunkerFactory({ fastAvgSize: 512 })

        // demo binary data
        const { buf, records: startRecords } = demoByteArray(RECORD_COUNT, RECORD_SIZE_BYTES)

        // persist chunked binary data
        const { root: origRoot, index: origIndex, blocks: origBlocks } = await create({ buf, chunk: fastcdc, encode })
        for (const block of origBlocks) await put(block)

        console.log(origIndex.indexStruct.startOffsets)

        const { indexStruct, indexBuffer } = await readIndex(origRoot, get, decode)

        console.log(indexStruct.startOffsets)

        assert.deepEqual(origIndex.indexStruct.startOffsets, indexStruct.startOffsets)
        assert.deepEqual(origIndex.indexStruct.indexSize, indexStruct.indexSize)
        assert.deepEqual(origIndex.indexStruct.byteArraySize, indexStruct.byteArraySize)
    })


    test("append and readIndex generate identical index structure", async () => {

        // configure chunky store
        const { get, put } = blockStore()
        const { encode, decode } = codec()
        const { create, read, append, readIndex } = chunkyStore()
        const { fastcdc } = chunkerFactory({ fastAvgSize: 512 })

        // demo binary data
        const { buf, records: startRecords } = demoByteArray(RECORD_COUNT, RECORD_SIZE_BYTES)

        // persist chunked binary data
        const { root: origRoot, index: origIndex, blocks: origBlocks } = await create({ buf, chunk: fastcdc, encode })
        for (const block of origBlocks) await put(block)

        const { indexStruct, indexBuffer } = await readIndex(origRoot, get, decode)

        assert.deepEqual(origIndex.indexStruct.startOffsets, indexStruct.startOffsets)
        assert.deepEqual(origIndex.indexStruct.indexSize, indexStruct.indexSize)
        assert.deepEqual(origIndex.indexStruct.byteArraySize, indexStruct.byteArraySize)

        // demo binary data to append
        const { buf: buf2, records: appendedRecords } = demoByteArray(RECORD_APPEND_COUNT, RECORD_SIZE_BYTES)
        // append binary data
        const { root: appendRoot, index: appendIndex, blocks: appendBlocks } = await append({ root: origRoot, decode, get }, { buf: buf2, chunk: fastcdc, encode })
        for (const block of appendBlocks) await put(block)

        console.log(appendIndex.indexStruct.startOffsets)

        const {indexStruct: readAppendIndex } = await readIndex(appendRoot, get, decode)

        console.log(readAppendIndex.startOffsets)

        assert.deepEqual(appendIndex.indexStruct.startOffsets, readAppendIndex.startOffsets)
        assert.deepEqual(appendIndex.indexStruct.indexSize, readAppendIndex.indexSize)
        assert.deepEqual(appendIndex.indexStruct.byteArraySize, readAppendIndex.byteArraySize)
    })

})

async function retrieveRecords(read: any, startOffset: number, recordCount: number, { root, decode, get }): Promise<any[]> {
    const completeBuffer = await read(startOffset, RECORD_SIZE_BYTES * recordCount, { root, decode, get })
    let cursor = 0
    const retrievedRecords = []
    for (let index = 0; index < recordCount; index++) {
        const recordBytes = completeBuffer.subarray(cursor * RECORD_SIZE_BYTES, cursor * RECORD_SIZE_BYTES + RECORD_SIZE_BYTES)
        const recordFound = uuidStringify(recordBytes)
        //const recordFound = new TextDecoder().decode(recordBytes)
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
        const bytes = uuidParse(demoRecord)
        //const bytes = new TextEncoder().encode(demoRecord);
        buf.set(bytes, cursor * RECORD_SIZE_BYTES);
        cursor++;
    }
    return { buf, records }
}

