
import { partReport } from '@dstanesc/fake-metrology-data'
import { simpleMaterialJson } from '@dstanesc/fake-material-data'
import { unpack, pack } from 'msgpackr'
import { sha256 } from 'multiformats/hashes/sha2'
import { v4 as uuidV4, parse as uuidParse, stringify as uuidStringify } from 'uuid';


import { codec, blockStore, chunkerFactory } from './util'
import { chunkyStore } from '../index'

import * as assert from 'assert';

const RECORD_APPEND_COUNT = 100
const RECORD_COUNT = 200
const RECORD_UPDATE_COUNT = 20
const RECORD_SIZE_BYTES = 36
const RECORD_UPDATE_POSITION = 100
const RECORD_UPDATE_POSITION_PRIOR_END = 170
const RECORD_UPDATE_OFFSET = RECORD_SIZE_BYTES * RECORD_UPDATE_POSITION
const RECORD_UPDATE_NEXT_OFFSET = RECORD_UPDATE_OFFSET + (RECORD_UPDATE_COUNT * RECORD_SIZE_BYTES)


describe("Chunky bulk", function () {


    test("compare discrete append and update w/ bulk append and single update", async () => {

        // demo binary data
        const { buf, records: startRecords } = demoByteArray(RECORD_COUNT, RECORD_SIZE_BYTES);

        /**
         * Discrete create append, update
         */

        // configure chunky store
        const { get, put } = blockStore()
        const { encode, decode } = codec()
        const { create, read, append, update, bulk } = chunkyStore()
        const { fastcdc } = chunkerFactory({ fastAvgSize: 512 })

        // initial  data
        const { root: origRoot, index: origIndex, blocks: origBlocks } = await create({ buf, chunk: fastcdc, encode })
        for (const block of origBlocks) await put(block)
        console.log(origIndex.indexStruct.startOffsets)

        // demo binary data to append
        const { buf: buf2, records: appendedRecords } = demoByteArray(RECORD_APPEND_COUNT, RECORD_SIZE_BYTES)
        // append binary data
        const { root: appendRoot, index: appendIndex, blocks: appendBlocks } = await append({ root: origRoot, decode, get }, { buf: buf2, chunk: fastcdc, encode })
        for (const block of appendBlocks) await put(block)
        console.log(appendIndex.indexStruct.startOffsets)

        const retrievedRecords1 = await retrieveRecords(read, 0, RECORD_COUNT, { root: origRoot, index: origIndex, decode, get })
        assert.equal(startRecords.length, retrievedRecords1.length)
        assert.deepEqual(startRecords, retrievedRecords1)

        const retrievedRecords2 = await retrieveRecords(read, 0, RECORD_COUNT + RECORD_APPEND_COUNT, { root: appendRoot, index: appendIndex, decode, get })

        const recordUnion = startRecords.concat(appendedRecords)
        assert.equal(recordUnion.length, retrievedRecords2.length)
        assert.deepEqual(recordUnion, retrievedRecords2)

        // demo binary data to update
        const { buf: buf3, records: updatingRecords } = demoByteArray(RECORD_UPDATE_COUNT, RECORD_SIZE_BYTES)

        // update from buf3 @ RECORD_UPDATE_OFFSET
        const { root: updateRoot, index: updateIndex, blocks: updateBlocks } = await update({ root: appendRoot, decode, get }, { buf: buf3, chunk: fastcdc, encode }, RECORD_UPDATE_OFFSET)
        for (const block of updateBlocks) await put(block)

        // read all discrete
        const allRecords = await retrieveRecords(read, 0, RECORD_COUNT + RECORD_APPEND_COUNT, { root: updateRoot, index: updateIndex, decode, get })

        /**
         *  Bulk append, update
         */
        const { root: bulkRoot, index: bulkIndex, blocks: bulkBlocks } = await bulk({ root: origRoot, decode, get, put }, { chunk: fastcdc, encode }, buf2, [{ updateBuffer: buf3, updateStartOffset: RECORD_UPDATE_OFFSET }])

        // read all bulk
        const bulkRecords = await retrieveRecords(read, 0, RECORD_COUNT + RECORD_APPEND_COUNT, { root: bulkRoot, index: bulkIndex, decode, get })

        assert.deepStrictEqual(allRecords, bulkRecords)
        assert.strictEqual(updateRoot.toString(), bulkRoot.toString())
        assert.deepStrictEqual(updateIndex.indexStruct, bulkIndex.indexStruct)
    })

    test("compare discrete append and update w/ bulk append and multiple updates", async () => {

        // demo binary data
        const { buf, records: startRecords } = demoByteArray(RECORD_COUNT, RECORD_SIZE_BYTES);

        /**
         * Discrete create + append + update + update
         */

        // configure chunky store
        const { get, put } = blockStore()
        const { encode, decode } = codec()
        const { create, read, append, update, bulk } = chunkyStore()
        const { fastcdc } = chunkerFactory({ fastAvgSize: 512 })

        // initial  data
        const { root: origRoot, index: origIndex, blocks: origBlocks } = await create({ buf, chunk: fastcdc, encode })
        for (const block of origBlocks) await put(block)

        // demo binary data to append
        const { buf: buf2, records: appendedRecords } = demoByteArray(RECORD_APPEND_COUNT, RECORD_SIZE_BYTES)
        // append binary data
        const { root: appendRoot, index: appendIndex, blocks: appendBlocks } = await append({ root: origRoot, decode, get }, { buf: buf2, chunk: fastcdc, encode })
        for (const block of appendBlocks) await put(block)

        const retrievedRecords1 = await retrieveRecords(read, 0, RECORD_COUNT, { root: origRoot, index: origIndex, decode, get })
        assert.equal(startRecords.length, retrievedRecords1.length)
        assert.deepEqual(startRecords, retrievedRecords1)

        const retrievedRecords2 = await retrieveRecords(read, 0, RECORD_COUNT + RECORD_APPEND_COUNT, { root: appendRoot, index: appendIndex, decode, get })

        const recordUnion = startRecords.concat(appendedRecords)
        assert.equal(recordUnion.length, retrievedRecords2.length)
        assert.deepEqual(recordUnion, retrievedRecords2)

        // demo binary data to update
        const { buf: buf3, records: updatingRecords } = demoByteArray(RECORD_UPDATE_COUNT, RECORD_SIZE_BYTES)

        // update from buf3 @ RECORD_UPDATE_OFFSET
        const { root: updateRoot, index: updateIndex, blocks: updateBlocks } = await update({ root: appendRoot, decode, get }, { buf: buf3, chunk: fastcdc, encode }, RECORD_UPDATE_OFFSET)
        for (const block of updateBlocks) await put(block)

        // demo binary data to update
        const { buf: buf4, records: updatingRecords2 } = demoByteArray(RECORD_UPDATE_COUNT - 10, RECORD_SIZE_BYTES)

        // update from buf4 @ RECORD_UPDATE_OFFSET + ( RECORD_UPDATE_COUNT * RECORD_SIZE_BYTES) ie. after first update
        const { root: updateRoot2, index: updateIndex2, blocks: updateBlocks2 } = await update({ root: updateRoot, decode, get }, { buf: buf4, chunk: fastcdc, encode }, RECORD_UPDATE_NEXT_OFFSET)
        for (const block of updateBlocks2) await put(block)

        // read all discrete
        const allRecords = await retrieveRecords(read, 0, RECORD_COUNT + RECORD_APPEND_COUNT, { root: updateRoot, index: updateIndex, decode, get })

        /**
         *  Create + bulk append and updates
         */
        const { root: bulkRoot, index: bulkIndex, blocks: bulkBlocks } = await bulk({ root: origRoot, decode, get,  put}, { chunk: fastcdc, encode }, buf2, [{ updateBuffer: buf3, updateStartOffset: RECORD_UPDATE_OFFSET }, { updateBuffer: buf4, updateStartOffset: RECORD_UPDATE_NEXT_OFFSET }])
        for (const block of bulkBlocks) await put(block)

        // read all bulk
        const bulkRecords = await retrieveRecords(read, 0, RECORD_COUNT + RECORD_APPEND_COUNT, { root: bulkRoot, index: bulkIndex, decode, get })

        assert.deepStrictEqual(allRecords, bulkRecords)
        assert.strictEqual(updateRoot2.toString(), bulkRoot.toString())
        assert.deepStrictEqual(updateIndex2.indexStruct, bulkIndex.indexStruct)
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