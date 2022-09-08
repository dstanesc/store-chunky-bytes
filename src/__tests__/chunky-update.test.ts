
import { partReport } from '@dstanesc/fake-metrology-data'
import { simpleMaterialJson } from '@dstanesc/fake-material-data'
import { unpack, pack } from 'msgpackr'
import { sha256 } from 'multiformats/hashes/sha2'
import { v4 as uuidV4, parse as uuidParse, stringify as uuidStringify } from 'uuid';


import { codec, blockStore, chunkerFactory } from './util'
import { chunkyStore } from '../index'

import * as assert from 'assert';

const RECORD_COUNT = 200
const RECORD_UPDATE_COUNT = 20
const RECORD_SIZE_BYTES = 36
const RECORD_UPDATE_POSITION = 100
const RECORD_UPDATE_OFFSET = RECORD_SIZE_BYTES * RECORD_UPDATE_POSITION



describe("Chunky update", function () {


    test("persist / update / read single", async () => {

        // demo binary data
        const { buf, records: startRecords } = demoByteArray(RECORD_COUNT, RECORD_SIZE_BYTES);

        // configure chunky store
        const { get, put } = blockStore()
        const { encode, decode } = codec()
        const { create, read, update } = chunkyStore()
        const { fastcdc, buzhash } = chunkerFactory({ fastAvgSize: 512 })

        // persist chunked binary data
        const { root, index, blocks } = await create({ buf, chunk: fastcdc, encode })
        blocks.forEach(block => put(block))


        // demo binary data to update
        const { buf: buf2, records: updatingRecord } = demoByteArray(1, RECORD_SIZE_BYTES)

        // update from buf2 @ RECORD_UPDATE_OFFSET
        const { root: updateRoot, index: updateIndex, blocks: updateBlocks } = await update({ root, decode, get }, { buf: buf2, chunk: fastcdc, encode }, RECORD_UPDATE_OFFSET)
        updateBlocks.forEach(block => put(block))

        // read one
        const updatedRecord = await retrieveRecords(read, RECORD_UPDATE_OFFSET, 1, { root: updateRoot, index: updateIndex, decode, get })

        console.log(updatingRecord)
        console.log(updatedRecord)

        assert.deepEqual(updatingRecord, updatedRecord)
    })


    test("persist / update / read multiple across update boundary", async () => {

        // demo binary data
        const { buf, records: startRecords } = demoByteArray(RECORD_COUNT, RECORD_SIZE_BYTES);

        // configure chunky store
        const { get, put } = blockStore()
        const { encode, decode } = codec()
        const { create, read, update } = chunkyStore()
        const { fastcdc, buzhash } = chunkerFactory({ fastAvgSize: 512 })

        // persist chunked binary data
        const { root, index, blocks } = await create({ buf, chunk: fastcdc, encode })
        blocks.forEach(block => put(block))


        // demo binary data to update
        const { buf: buf2, records: updatingRecords } = demoByteArray(RECORD_UPDATE_COUNT, RECORD_SIZE_BYTES)

        // update from buf2 @ RECORD_UPDATE_OFFSET
        const { root: updateRoot, index: updateIndex, blocks: updateBlocks } = await update({ root, decode, get }, { buf: buf2, chunk: fastcdc, encode }, RECORD_UPDATE_OFFSET)
        updateBlocks.forEach(block => put(block))

        // read one
        const updatedRecords = await retrieveRecords(read, RECORD_UPDATE_OFFSET, RECORD_UPDATE_COUNT + 10, { root: updateRoot, index: updateIndex, decode, get })

        console.log(updatingRecords)
        console.log(updatedRecords)

        assert.deepEqual(updatingRecords, updatedRecords.slice(0, 20))
        assert.deepEqual(startRecords.slice(RECORD_UPDATE_OFFSET + 20, RECORD_UPDATE_OFFSET + 30), updatedRecords.slice(20, 10))
    })

    test("persist / update at beginning", async () => {

        // demo binary data
        const { buf, records: startRecords } = demoByteArray(RECORD_COUNT, RECORD_SIZE_BYTES);

        // configure chunky store
        const { get, put } = blockStore()
        const { encode, decode } = codec()
        const { create, read, update } = chunkyStore()
        const { fastcdc, buzhash } = chunkerFactory({ fastAvgSize: 512 })

        // persist chunked binary data
        const { root, index, blocks } = await create({ buf, chunk: fastcdc, encode })
        blocks.forEach(block => put(block))


        // demo binary data to update
        const { buf: buf2, records: updatingRecords } = demoByteArray(RECORD_UPDATE_COUNT, RECORD_SIZE_BYTES)

        // update from buf2 @ RECORD_UPDATE_OFFSET
        const { root: updateRoot, index: updateIndex, blocks: updateBlocks } = await update({ root, decode, get }, { buf: buf2, chunk: fastcdc, encode }, 0)
        updateBlocks.forEach(block => put(block))

        // read one
        const updatedRecords = await retrieveRecords(read, 0, RECORD_UPDATE_COUNT + 10, { root: updateRoot, index: updateIndex, decode, get })

        console.log(updatingRecords)
        console.log(updatedRecords)

        assert.deepEqual(updatingRecords, updatedRecords.slice(0, 20))
        assert.deepEqual(startRecords.slice(20, 30), updatedRecords.slice(20, 30))
    })



    test("persist / update debug", async () => {

        // demo binary data
        const { buf, records: startRecords } = demoByteArray(RECORD_COUNT, RECORD_SIZE_BYTES);


        // configure chunky store
        const { get, put } = blockStore()
        const { encode, decode } = codec()
        const { create, read, update } = chunkyStore()
        const { fastcdc, buzhash } = chunkerFactory({ fastAvgSize: 512 })

        // persist chunked binary data
        const { root, index, blocks } = await create({ buf, chunk: fastcdc, encode })
        blocks.forEach(block => put(block))


        // demo binary data to update
        const { buf: buf2 } = demoByteArray(RECORD_UPDATE_COUNT, RECORD_SIZE_BYTES)

        //const updateOffset = RECORD_UPDATE_OFFSET

        const { root: updateRoot, index: updateIndex, blocks: updateBlocks } = await update({ root, decode, get }, { buf: buf2, chunk: fastcdc, encode }, RECORD_UPDATE_OFFSET)
        updateBlocks.forEach(block => put(block))

        console.log(`Index struct size ${updateIndex.indexStruct.indexSize} byte array ${updateIndex.indexStruct.byteArraySize}`)

        console.log(updateIndex.indexStruct.startOffsets)

        const indexBuffer = updateIndex.indexBuffer
        const INDEX_HEADER_SIZE: number = 12 // bytes |<-- index control (4 bytes) -->|<-- index size (4 bytes) -->|<-- byte array size (4 bytes) -->|
        const INDEX_BLOCK_SIZE: number = 40 // bytes |<-- chunk relative offset (4 bytes) -->|<-- chunk CID (36 bytes) -->|

        const blockSize = INDEX_BLOCK_SIZE
        const shift = INDEX_HEADER_SIZE
        let pos = shift
        const startOffsets = new Map()
        let absoluteOffset = 0

        const indexSize = readUInt(indexBuffer, 4)
        const byteArraySize = readUInt(indexBuffer, 8)

        console.log(`Index binary size ${indexSize} byte array ${byteArraySize}`)

        for (let i = 0; i < updateIndex.indexStruct.indexSize; i++) {
            const relativeStartOffset = readUInt(indexBuffer, pos)
            absoluteOffset += relativeStartOffset
            const cidBytes = readBytes(indexBuffer, pos + 4, 36)
            const chunkCid = decode(cidBytes)
            startOffsets.set(absoluteOffset, chunkCid)
            pos += blockSize
        }

        console.log(startOffsets)
    })

    test("persist / update / read full validation", async () => {


        // demo binary data
        const { buf, records: startRecords } = demoByteArray(RECORD_COUNT, RECORD_SIZE_BYTES);

        // configure chunky store
        const { get, put } = blockStore()
        const { encode, decode } = codec()
        const { create, read, update } = chunkyStore()
        const { fastcdc, buzhash } = chunkerFactory({ fastAvgSize: 512 })

        // persist chunked binary data
        const { root, index, blocks } = await create({ buf, chunk: fastcdc, encode })
        blocks.forEach(block => put(block))


        // demo binary data to update
        const { buf: buf2, records: updatingRecords } = demoByteArray(RECORD_UPDATE_COUNT, RECORD_SIZE_BYTES)

        // update from buf2 @ RECORD_UPDATE_OFFSET
        const { root: updateRoot, index: updateIndex, blocks: updateBlocks } = await update({ root, decode, get }, { buf: buf2, chunk: fastcdc, encode }, RECORD_UPDATE_OFFSET)
        updateBlocks.forEach(block => put(block))

        // read all
        const updatedRecords = await retrieveRecords(read, 0, RECORD_COUNT, { root: updateRoot, index: updateIndex, decode, get })
        assert.equal(startRecords.length, updatedRecords.length)

        // validate unmodified 
        const unmodifiedSliceBegin = updatedRecords.slice(0, RECORD_UPDATE_POSITION)
        const originalSliceBegin = startRecords.slice(0, RECORD_UPDATE_POSITION)

        assert.deepEqual(originalSliceBegin, unmodifiedSliceBegin)

        // validate modified
        const modifiedSlice = updatedRecords.slice(RECORD_UPDATE_POSITION, RECORD_UPDATE_POSITION + RECORD_UPDATE_COUNT)

        assert.equal(updatingRecords.length, modifiedSlice.length)

        assert.deepEqual(updatingRecords, modifiedSlice)

        // validate unmodified
        const unmodifiedSliceEnd = updatedRecords.slice(RECORD_UPDATE_POSITION + RECORD_UPDATE_COUNT, updatedRecords.length)
        const originalSliceEnd = startRecords.slice(RECORD_UPDATE_POSITION + RECORD_UPDATE_COUNT, startRecords.length)

        assert.equal(unmodifiedSliceEnd.length, originalSliceEnd.length)

        assert.deepEqual(unmodifiedSliceEnd, originalSliceEnd)
    })
})


const readUInt = (buffer: Uint8Array, pos: number): number => {
    const value = ((buffer[pos]) |
        (buffer[pos + 1] << 8) |
        (buffer[pos + 2] << 16)) +
        (buffer[pos + 3] * 0x1000000)
    return value
}

const readBytes = (buffer: Uint8Array, pos: number, length: number): Uint8Array => {
    const bytes = buffer.subarray(pos, pos + length)
    return bytes
}

function demoByteArray(recordCount: number, recordSizeBytes: number): { buf: Uint8Array, records: any[] } {
    const buf = new Uint8Array(recordCount * recordSizeBytes);
    let cursor = 0;
    const records = [];
    for (let index = 0; index < recordCount; index++) {
        const demoRecord = uuidV4();
        records.push(demoRecord);
        const bytes = uuidParse(demoRecord)
        buf.set(bytes, cursor * recordSizeBytes);
        cursor++;
    }
    return { buf, records }
}


async function retrieveRecords(read: any, startOffset: number, recordCount: number, { root, index, decode, get }): Promise<any[]> {
    const completeBuffer = await read(startOffset, RECORD_SIZE_BYTES * recordCount, { root, decode, get })
    let cursor = 0
    const retrievedRecords = []
    for (let index = 0; index < recordCount; index++) {
        const recordBytes = completeBuffer.subarray(cursor * RECORD_SIZE_BYTES, cursor * RECORD_SIZE_BYTES + RECORD_SIZE_BYTES)
        const recordFound = uuidStringify(recordBytes)
        retrievedRecords.push(recordFound)
        cursor++
    }
    return retrievedRecords
}