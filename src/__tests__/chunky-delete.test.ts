
import { partReport } from '@dstanesc/fake-metrology-data'
import { simpleMaterialJson } from '@dstanesc/fake-material-data'
import { unpack, pack } from 'msgpackr'
import { sha256 } from 'multiformats/hashes/sha2'
import { v4 as uuidV4, parse as uuidParse, stringify as uuidStringify } from 'uuid';


import { codec, blockStore, chunkerFactory } from './util'
import { chunkyStore } from '../index'

import * as assert from 'assert';

const RECORD_COUNT = 200
const RECORD_DELETE_COUNT = 30
const RECORD_SIZE_BYTES = 36

const RECORD_DELETE_POSITION = 100
const RECORD_DELETE_OFFSET = RECORD_SIZE_BYTES * RECORD_DELETE_POSITION


describe("Chunky update", function () {

    test("persist / delete / read all & validate deletion, start delete", async () => {

        // demo binary data
        const { buf, records: startRecords } = demoByteArray(RECORD_COUNT, RECORD_SIZE_BYTES);

        // configure chunky store
        const { get, put } = blockStore()
        const { encode, decode } = codec()
        const { create, read, remove } = chunkyStore()
        const { fastcdc, buzhash } = chunkerFactory({ fastAvgSize: 512 })

        // persist chunked binary data
        const { root, index, blocks } = await create({ buf, chunk: fastcdc, encode })
        blocks.forEach(block => put(block))

        const { root: deleteRoot, index: deleteIndex, blocks: deleteBlocks } = await remove({ root, decode, get }, { chunk: fastcdc, encode }, 0, RECORD_DELETE_COUNT * RECORD_SIZE_BYTES)
        deleteBlocks.forEach(block => put(block))

        let remainingBytesChecksum = 0
        for (const cid of deleteIndex.indexStruct.startOffsets.values()) {
            //console.log(cid)
            const buf = await get(cid)
            remainingBytesChecksum += buf.byteLength
        }
        console.log(`Byte checksum is ${remainingBytesChecksum}`)

        const expectedBytesAfterDelete = index.indexStruct.byteArraySize - RECORD_DELETE_COUNT * RECORD_SIZE_BYTES
        assert.equal(expectedBytesAfterDelete, remainingBytesChecksum)

        // read all
        const remainingRecords = await retrieveRecords(read, 0, RECORD_COUNT - RECORD_DELETE_COUNT, { root: deleteRoot, index: deleteIndex, decode, get })

        console.log(remainingRecords.length)

        assert.equal(RECORD_COUNT - RECORD_DELETE_COUNT, remainingRecords.length)

        assert.deepEqual(startRecords.slice(RECORD_DELETE_COUNT, RECORD_COUNT), remainingRecords)
    })

    test("persist / delete / read all & validate deletion, central delete", async () => {

        // demo binary data
        const { buf, records: startRecords } = demoByteArray(RECORD_COUNT, RECORD_SIZE_BYTES);

        // configure chunky store
        const { get, put } = blockStore()
        const { encode, decode } = codec()
        const { create, read, remove } = chunkyStore()
        const { fastcdc, buzhash } = chunkerFactory({ fastAvgSize: 512 })

        // persist chunked binary data
        const { root, index, blocks } = await create({ buf, chunk: fastcdc, encode })
        blocks.forEach(block => put(block))

        const { root: deleteRoot, index: deleteIndex, blocks: deleteBlocks } = await remove({ root, decode, get }, { chunk: fastcdc, encode }, RECORD_DELETE_OFFSET, RECORD_DELETE_COUNT * RECORD_SIZE_BYTES)
        deleteBlocks.forEach(block => put(block))

        let remainingBytesChecksum = 0
        for (const cid of deleteIndex.indexStruct.startOffsets.values()) {
            //console.log(cid)
            const buf = await get(cid)
            remainingBytesChecksum += buf.byteLength
        }
        console.log(`Byte checksum is ${remainingBytesChecksum}`)

        const expectedBytesAfterDelete = index.indexStruct.byteArraySize - RECORD_DELETE_COUNT * RECORD_SIZE_BYTES
        assert.equal(expectedBytesAfterDelete, remainingBytesChecksum)

        // read all
        const remainingRecords = await retrieveRecords(read, 0, RECORD_COUNT - RECORD_DELETE_COUNT, { root: deleteRoot, index: deleteIndex, decode, get })

        console.log(remainingRecords.length)

        assert.deepEqual(startRecords.slice(0, RECORD_DELETE_POSITION), remainingRecords.slice(0, RECORD_DELETE_POSITION))

        assert.deepEqual(startRecords.slice(RECORD_DELETE_POSITION + RECORD_DELETE_COUNT, RECORD_COUNT), remainingRecords.slice(RECORD_DELETE_POSITION, RECORD_COUNT - RECORD_DELETE_COUNT))

        assert.equal(RECORD_COUNT - RECORD_DELETE_COUNT, remainingRecords.length)
    })


    test("persist / delete / read all & validate deletion, end delete", async () => {

        // demo binary data
        const { buf, records: startRecords } = demoByteArray(RECORD_COUNT, RECORD_SIZE_BYTES);

        // configure chunky store
        const { get, put } = blockStore()
        const { encode, decode } = codec()
        const { create, read, remove } = chunkyStore()
        const { fastcdc, buzhash } = chunkerFactory({ fastAvgSize: 512 })

        // persist chunked binary data
        const { root, index, blocks } = await create({ buf, chunk: fastcdc, encode })
        blocks.forEach(block => put(block))

        const { root: deleteRoot, index: deleteIndex, blocks: deleteBlocks } = await remove({ root, decode, get }, { chunk: fastcdc, encode }, (RECORD_COUNT - RECORD_DELETE_COUNT) * RECORD_SIZE_BYTES, RECORD_DELETE_COUNT * RECORD_SIZE_BYTES)
        deleteBlocks.forEach(block => put(block))

        let remainingBytesChecksum = 0
        for (const cid of deleteIndex.indexStruct.startOffsets.values()) {
            //console.log(cid)
            const buf = await get(cid)
            remainingBytesChecksum += buf.byteLength
        }
        console.log(`Byte checksum is ${remainingBytesChecksum}`)

        const expectedBytesAfterDelete = index.indexStruct.byteArraySize - RECORD_DELETE_COUNT * RECORD_SIZE_BYTES
        assert.equal(expectedBytesAfterDelete, remainingBytesChecksum)

        // read all
        const remainingRecords = await retrieveRecords(read, 0, RECORD_COUNT - RECORD_DELETE_COUNT, { root: deleteRoot, index: deleteIndex, decode, get })

        assert.equal(RECORD_COUNT - RECORD_DELETE_COUNT, remainingRecords.length)

        assert.deepEqual(startRecords.slice(0, RECORD_COUNT - RECORD_DELETE_COUNT), remainingRecords)
    })

    test("chunk stability after full re-chunking, central delete", async () => {
        // demo binary data
        const { buf, records: startRecords } = demoByteArray(RECORD_COUNT, RECORD_SIZE_BYTES);

        // configure chunky store
        const { get, put } = blockStore()
        const { encode, decode } = codec()
        const { create, read, remove } = chunkyStore()
        const { fastcdc, buzhash } = chunkerFactory({ fastAvgSize: 512 })

        // persist chunked binary data
        const { root, index, blocks } = await create({ buf, chunk: fastcdc, encode })
        blocks.forEach(block => put(block))

        const { root: deleteRoot, index: deleteIndex, blocks: deleteBlocks } = await remove({ root, decode, get }, { chunk: fastcdc, encode }, RECORD_DELETE_OFFSET, RECORD_DELETE_COUNT * RECORD_SIZE_BYTES)
        deleteBlocks.forEach(block => put(block))

        // read full buffer
        const deletedBuffer = await read(0, (RECORD_COUNT - RECORD_DELETE_COUNT) * RECORD_SIZE_BYTES, { root: deleteRoot, decode, get })

        // persist again full updated buffer
        const { root: reRoot, index: reIndex, blocks: reBlocks } = await create({ buf: deletedBuffer, chunk: fastcdc, encode })
        reBlocks.forEach(block => put(block))

        // read full buffer again to assert binary equality
        const reBuffer = await read(0, (RECORD_COUNT - RECORD_DELETE_COUNT) * RECORD_SIZE_BYTES, { root: reRoot, decode, get })
        assert.deepEqual(deletedBuffer, reBuffer)

        // read all remaining after deletion
        const remainingRecords = await retrieveRecords(read, 0, RECORD_COUNT - RECORD_DELETE_COUNT, { root: deleteRoot, index: deleteIndex, decode, get })
        assert.equal(startRecords.length - RECORD_DELETE_COUNT, remainingRecords.length)

        // read again & test equality @ domain level
        const reRecords = await retrieveRecords(read, 0, RECORD_COUNT - RECORD_DELETE_COUNT, { root: reRoot, index: reIndex, decode, get })
        assert.equal(reRecords.length, remainingRecords.length)
        assert.deepEqual(reRecords, remainingRecords)

        console.log(reIndex.indexStruct.startOffsets)

        assert.equal(deleteIndex.indexStruct.indexSize, reIndex.indexStruct.indexSize)
        assert.deepEqual(deleteIndex.indexStruct.startOffsets, reIndex.indexStruct.startOffsets)
    })


    test("chunk stability after full re-chunking, start delete", async () => {
        // demo binary data
        const { buf, records: startRecords } = demoByteArray(RECORD_COUNT, RECORD_SIZE_BYTES);

        // configure chunky store
        const { get, put } = blockStore()
        const { encode, decode } = codec()
        const { create, read, remove } = chunkyStore()
        const { fastcdc, buzhash } = chunkerFactory({ fastAvgSize: 512 })

        // persist chunked binary data
        const { root, index, blocks } = await create({ buf, chunk: fastcdc, encode })
        blocks.forEach(block => put(block))

        const { root: deleteRoot, index: deleteIndex, blocks: deleteBlocks } = await remove({ root, decode, get }, { chunk: fastcdc, encode }, 0, RECORD_DELETE_COUNT * RECORD_SIZE_BYTES)
        deleteBlocks.forEach(block => put(block))

        // read full buffer
        const deletedBuffer = await read(0, (RECORD_COUNT - RECORD_DELETE_COUNT) * RECORD_SIZE_BYTES, { root: deleteRoot, decode, get })

        // persist again full updated buffer
        const { root: reRoot, index: reIndex, blocks: reBlocks } = await create({ buf: deletedBuffer, chunk: fastcdc, encode })
        reBlocks.forEach(block => put(block))

        // read full buffer again to assert binary equality
        const reBuffer = await read(0, (RECORD_COUNT - RECORD_DELETE_COUNT) * RECORD_SIZE_BYTES, { root: reRoot, decode, get })
        assert.deepEqual(deletedBuffer, reBuffer)

        // read all remaining after deletion
        const remainingRecords = await retrieveRecords(read, 0, RECORD_COUNT - RECORD_DELETE_COUNT, { root: deleteRoot, index: deleteIndex, decode, get })
        assert.equal(startRecords.length - RECORD_DELETE_COUNT, remainingRecords.length)

        // read again & test equality @ domain level
        const reRecords = await retrieveRecords(read, 0, RECORD_COUNT - RECORD_DELETE_COUNT, { root: reRoot, index: reIndex, decode, get })
        assert.equal(reRecords.length, remainingRecords.length)
        assert.deepEqual(reRecords, remainingRecords)

        console.log(reIndex.indexStruct.startOffsets)

        assert.equal(deleteIndex.indexStruct.indexSize, reIndex.indexStruct.indexSize)
        assert.deepEqual(deleteIndex.indexStruct.startOffsets, reIndex.indexStruct.startOffsets)
    })


    test("chunk stability after full re-chunking, end delete", async () => {
        // demo binary data
        const { buf, records: startRecords } = demoByteArray(RECORD_COUNT, RECORD_SIZE_BYTES);

        // configure chunky store
        const { get, put } = blockStore()
        const { encode, decode } = codec()
        const { create, read, remove } = chunkyStore()
        const { fastcdc, buzhash } = chunkerFactory({ fastAvgSize: 512 })

        // persist chunked binary data
        const { root, index, blocks } = await create({ buf, chunk: fastcdc, encode })
        blocks.forEach(block => put(block))

        const { root: deleteRoot, index: deleteIndex, blocks: deleteBlocks } = await remove({ root, decode, get }, { chunk: fastcdc, encode }, (RECORD_COUNT - RECORD_DELETE_COUNT) * RECORD_SIZE_BYTES, RECORD_DELETE_COUNT * RECORD_SIZE_BYTES)
        deleteBlocks.forEach(block => put(block))

        // read full buffer
        const deletedBuffer = await read(0, (RECORD_COUNT - RECORD_DELETE_COUNT) * RECORD_SIZE_BYTES, { root: deleteRoot, decode, get })

        // persist again full updated buffer
        const { root: reRoot, index: reIndex, blocks: reBlocks } = await create({ buf: deletedBuffer, chunk: fastcdc, encode })
        reBlocks.forEach(block => put(block))

        // read full buffer again to assert binary equality
        const reBuffer = await read(0, (RECORD_COUNT - RECORD_DELETE_COUNT) * RECORD_SIZE_BYTES, { root: reRoot, decode, get })
        assert.deepEqual(deletedBuffer, reBuffer)

        // read all remaining after deletion
        const remainingRecords = await retrieveRecords(read, 0, RECORD_COUNT - RECORD_DELETE_COUNT, { root: deleteRoot, index: deleteIndex, decode, get })
        assert.equal(startRecords.length - RECORD_DELETE_COUNT, remainingRecords.length)

        // read again & test equality @ domain level
        const reRecords = await retrieveRecords(read, 0, RECORD_COUNT - RECORD_DELETE_COUNT, { root: reRoot, index: reIndex, decode, get })
        assert.equal(reRecords.length, remainingRecords.length)
        assert.deepEqual(reRecords, remainingRecords)

        console.log(reIndex.indexStruct.startOffsets)

        assert.equal(deleteIndex.indexStruct.indexSize, reIndex.indexStruct.indexSize)
        assert.deepEqual(deleteIndex.indexStruct.startOffsets, reIndex.indexStruct.startOffsets)
    })

    test("persist / delete / block reuse", async () => {

        // demo binary data
        const { buf, records: startRecords } = demoByteArray(RECORD_COUNT, RECORD_SIZE_BYTES);

        // configure chunky store
        const { get, put } = blockStore()
        const { encode, decode } = codec()
        const { create, read, remove } = chunkyStore()
        const { fastcdc, buzhash } = chunkerFactory({ fastAvgSize: 512 })

        // persist chunked binary data
        const { root, index, blocks } = await create({ buf, chunk: fastcdc, encode })
        blocks.forEach(block => put(block))

        const { root: deleteRoot, index: deleteIndex, blocks: deleteBlocks } = await remove({ root, decode, get }, { chunk: fastcdc, encode }, RECORD_DELETE_OFFSET, RECORD_DELETE_COUNT * RECORD_SIZE_BYTES)
        deleteBlocks.forEach(block => put(block))
        const origBlocks = Array.from( index.indexStruct.startOffsets.values()).map(cid => cid.toString())
        const remainingBlocks =  Array.from( deleteIndex.indexStruct.startOffsets.values()).map(cid => cid.toString())
       
        let reuse = remainingBlocks.filter(x => origBlocks.includes(x))
        let diff = remainingBlocks.filter(x => !origBlocks.includes(x))

        console.log(`Reuse:${reuse.length} Diff"${diff.length}`)
        
        assert.ok(reuse.length > diff.length)
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
    const completeBuffer = await read(startOffset, RECORD_SIZE_BYTES * recordCount, { root, decode, index, get })
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