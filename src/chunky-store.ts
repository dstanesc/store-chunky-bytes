import bounds from 'binary-search-bounds'

const INDEX_CONTROL_FLAG: number = 0b100100 // Dec 36, Hex 0x24

const INDEX_HEADER_SIZE: number = 12 // bytes |<-- index control (4 bytes) -->|<-- index size (4 bytes) -->|<-- byte array size (4 bytes) -->|

const INDEX_BLOCK_SIZE: number = 40 // bytes |<-- chunk start offset (4 bytes) -->|<-- chunk CID (36 bytes) -->|

const CHUNK_CONTENT_IDENTIFIER_SIZE: number = 36 // bytes

const writeControlFlag = (buffer: Uint8Array, pos: number, controlFlag: number): number => {
    let flag = 0
    flag |= controlFlag
    return writeUInt(buffer, pos, flag)
}

const readControlFlag = (buffer: Uint8Array, pos: number): number => {
    return readUInt(buffer, pos)
}

const writeUInt = (buffer: Uint8Array, pos: number, value: number): number => {
    if (value < 0 || value > 0xffffffff) throw new Error("Integer out of range")
    buffer[pos] = (value & 0xff)
    buffer[pos + 1] = (value >>> 8)
    buffer[pos + 2] = (value >>> 16)
    buffer[pos + 3] = (value >>> 24)
    return pos + 4
}

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


const chunkyStore = () => {

    /*
     *  Create chunks out of the input buffer
     *
     *  @param {Uint8Array} buff - Input buffer to partition in (content defined) chunks
     *  @param {(data: Uint8Array) => Uint32Array} chunk - Chunking algorithm to apply on the input. Should return a list of chunk start offsets
     *  @param {(chunkBytes: Uint8Array) => Promise<any> } encode - Cid encoding function
     *  
     *  @returns {{any, any,  {cid: any, bytes: Uint8Array }[]} } root, index, blocks - A data structure containing the chunks (to persist) and the root handle
     */
    const create = async ({ buf, chunk, encode }: { buf: Uint8Array, chunk: (data: Uint8Array) => Uint32Array, encode: (chunkBytes: Uint8Array) => Promise<any> }): Promise<{ root: any, index: { startOffsets: Map<number, any>, indexSize: number, byteArraySize: number }, blocks: { cid: any, bytes: Uint8Array }[] }> => {
        const offsets = chunk(buf)
        const shift = INDEX_HEADER_SIZE // allow index header
        const blockSize = INDEX_BLOCK_SIZE
        let lastOffset = 0
        let pos = shift
        const startOffsets: Map<number, any> = new Map()
        const blocks: { cid: any, bytes: Uint8Array }[] = [] // {cid, bytes}
        //const endOffsets = new Map()
        const indexSize: number = offsets.length
        const byteArraySize: number = buf.length
        const index: { startOffsets: Map<number, any>, indexSize: number, byteArraySize: number } = { startOffsets /*, endOffsets*/, indexSize: indexSize, byteArraySize: byteArraySize }
        const indexBuffer = new Uint8Array(indexSize * blockSize + shift)
        for (const offset of offsets.values()) {
            const chunkBytes = buf.subarray(lastOffset, offset)
            const chunkCid = await encode(chunkBytes)
            if (chunkCid.byteLength !== CHUNK_CONTENT_IDENTIFIER_SIZE) throw new Error(`The cid returned by 'encode' function has unexpected size ${chunkCid.byteLength}. Expected 36 bytes.`)
            const block = { cid: chunkCid, bytes: chunkBytes }
            blocks.push(block)
            startOffsets.set(lastOffset, chunkCid)
            // endOffsets.set(lastOffset, offset - 1)
            // TODO store chunk length vs. absolute offset 
            // Propagate choice to the rust library
            writeUInt(indexBuffer, pos, lastOffset)
            indexBuffer.set(chunkCid.bytes, pos + 4)
            lastOffset = offset
            pos += blockSize
        }

        writeControlFlag(indexBuffer, 0, INDEX_CONTROL_FLAG) // index control
        writeUInt(indexBuffer, 4, indexSize)  // index size
        writeUInt(indexBuffer, 8, byteArraySize)  // byte array size

        const root = await encode(indexBuffer)
        if (root.byteLength !== 36) throw new Error(`The cid returned by 'encode' function has unexpected size ${indexBuffer.byteLength}, Expected 36 bytes.`)

        // TODO chunk index on size threshold, fixed size chunks
        const rootBlock = { cid: root, bytes: indexBuffer }
        blocks.push(rootBlock)

        return { root, index, blocks }
    }


    const relevantChunks = (startOffsetArray: any[], startOffset: number, endOffset: number): any[] => {

        return startOffsetArray.slice(bounds.le(startOffsetArray, startOffset), bounds.ge(startOffsetArray, endOffset) + 1)
    }

    /*
     *  Read a slice of the data across chunk boundaries
     *
     *  @param {number} startOffset - Zero-based valid index at which to begin extraction.
     *  @param {number} length - Number of bytes to read
     *  @param {any} root - THe rood content identifier as returned by the `create` function
     *  @param {{ startOffsets: Map<number, any>, indexSize: number, byteArraySize: number }} index (optional) - the cached index as returned by the `create` function
     *  @param {(cidBytes: Uint8Array) => any} decode - Cid decoding function
     *  @param {(cid: any) => Promise<Uint8Array> }} get - data block access function
     *  @returns {Uint8Array} - Requested slice of data
     */
    const read = async (startOffset: number, length: number, { root, index, decode, get }: { root?: any, index?: any, decode: (cidBytes: Uint8Array) => any, get: (cid: any) => Promise<Uint8Array> }, debugCallback?: Function): Promise<Uint8Array> => {

        if (index === undefined) {
            if (root === undefined) throw new Error(`Missing root, please provide an index or root as arg`)
            const { indexStruct } = await readIndex(root, get, decode)
            index = indexStruct
        }
        const endOffset = startOffset + length
        if (startOffset > index.byteArraySize) throw new Error(`Start offset out of range ${startOffset} > buffer size ${index.byteArraySize}`)
        if (endOffset > index.byteArraySize) throw new Error(`End offset out of range ${endOffset} > buffer size ${index.byteArraySize}`)
        const startOffsetsIndexed = index.startOffsets
        const startOffsetArray = Array.from(startOffsetsIndexed.keys())
        const selectedChunks = relevantChunks(startOffsetArray, startOffset, endOffset)
        const resultBuffer: Uint8Array = new Uint8Array(length)
        let cursor = 0
        let blocksLoaded = 0
        for (let i = 0; i < selectedChunks.length; i++) {
            blocksLoaded++
            const chunkOffset = selectedChunks[i]
            const chunkCid = startOffsetsIndexed.get(chunkOffset)
            //const chunkBuffer = await blockStore.get(chunkCid)
            const chunkBuffer = await get(chunkCid)
            if (chunkOffset <= startOffset && endOffset < chunkOffset + chunkBuffer.byteLength) {
                // single block read
                resultBuffer.set(chunkBuffer.subarray(startOffset - chunkOffset, endOffset - chunkOffset), cursor)
                cursor += endOffset - startOffset
                break
            } else if (chunkOffset <= startOffset) {
                // first block 
                resultBuffer.set(chunkBuffer.subarray(startOffset - chunkOffset, chunkBuffer.byteLength), cursor)
                cursor = chunkBuffer.byteLength - (startOffset - chunkOffset)
            } else if (chunkOffset > startOffset && endOffset > chunkOffset + chunkBuffer.byteLength) {
                // full block
                resultBuffer.set(chunkBuffer, cursor)
                cursor += chunkBuffer.byteLength
            } else if (chunkOffset > startOffset && endOffset <= chunkOffset + chunkBuffer.byteLength) {
                // last block
                resultBuffer.set(chunkBuffer.subarray(0, endOffset - chunkOffset), cursor)
                cursor += endOffset - chunkOffset
                break
            }
        }

        if (debugCallback) {

            debugCallback({ blocksLoaded })
        }

        if (cursor !== resultBuffer.byteLength) throw new Error(`alg. error, check code cursor=${cursor}, resultBuffer=${resultBuffer.byteLength}`)

        return resultBuffer
    }

    /*
     *  Append the input buffer to an existing chunked array. The behavior is correct only if:
     *
     *  1. the same chunking algorithm is used as in the original `create`
     *  2. the chunking algorithm is content-defined
     * 
     *  If above conditions are met, the function will return the incremental blocks and a new root. 
     *  The new root can be used to read any slice of data from the combined byte arrays.
     *  
     *  @param {any} root - The rood content identifier of a previous chunked array as returned by the `create` function
     *  @param {(cidBytes: Uint8Array) => any} decode - Cid decoding function
     *  @param {(cid: any) => Promise<Uint8Array> }} get - data block access function
     *  @param {Uint8Array} buff - Input buffer to partition in (content defined) chunks
     *  @param {(data: Uint8Array) => Uint32Array} chunk - Chunking algorithm to apply on the input. Should return a list of chunk start offsets
     *  @param {(chunkBytes: Uint8Array) => Promise<any> } encode - Cid encoding function
     *  
     *  @returns {{any, any,  {cid: any, bytes: Uint8Array }[]} } root, index, blocks - A data structure containing the chunks (to persist) and the root handle
     */
    const append = async ({ root, decode, get }: { root?: any, decode: (cidBytes: Uint8Array) => any, get: (cid: any) => Promise<Uint8Array> }, { buf, chunk, encode }: { buf: Uint8Array, chunk: (data: Uint8Array) => Uint32Array, encode: (chunkBytes: Uint8Array) => Promise<any> }): Promise<{ root: any, index: { startOffsets: Map<number, any>, indexSize: number, byteArraySize: number }, blocks: { cid: any, bytes: Uint8Array }[] }> => {
        
        if (root === undefined) throw new Error(`Missing root, please provide the predecessor root as arg`)
        const { indexStruct: origIndex, indexBuffer: origIndexBuffer } = await readIndex(root, get, decode)
        const { startOffsets: origStartOffsets, indexSize: origIndexSize, byteArraySize: origByteArraySize } = origIndex
        const origStartOffsetArray: number[] = Array.from(origStartOffsets.keys())
        const lastChunkOffset = origStartOffsetArray[origStartOffsetArray.length - 1]
        const lastChunkCid = origStartOffsets.get(lastChunkOffset)
        const lastChunkBuffer = await get(lastChunkCid)
        const adjust = (offset: number): number => offset + lastChunkOffset
        const overlapBuffer = new Uint8Array(lastChunkBuffer.byteLength + buf.byteLength)
        overlapBuffer.set(lastChunkBuffer, 0)
        overlapBuffer.set(buf, lastChunkBuffer.byteLength)
        const appendOffsets = chunk(overlapBuffer)
        const appendBlocks: { cid: any, bytes: Uint8Array }[] = [] // {cid, bytes}
        let lastOffset = 0
        const shift = INDEX_HEADER_SIZE 
        const blockSize = INDEX_BLOCK_SIZE
        let pos = (origIndexSize - 1) * blockSize + shift
        const appendStartOffsets: Map<number, any> = new Map(origIndex.startOffsets)
        const appendIndexSize: number = origIndexSize + appendOffsets.length - 1
        const appendByteArraySize: number = origByteArraySize + buf.length
        const appendIndex: { startOffsets: Map<number, any>, indexSize: number, byteArraySize: number } = { startOffsets: appendStartOffsets /*, endOffsets*/, indexSize: appendIndexSize, byteArraySize: appendByteArraySize }
        const appendIndexBuffer = new Uint8Array(appendIndexSize * blockSize + shift)
        for (const offset of appendOffsets.values()) {
            const chunkBytes = overlapBuffer.subarray(lastOffset, offset)
            const chunkCid = await encode(chunkBytes)
            if (chunkCid.byteLength !== CHUNK_CONTENT_IDENTIFIER_SIZE) throw new Error(`The cid returned by 'encode' function has unexpected size ${chunkCid.byteLength}. Expected 36 bytes.`)
            const block = { cid: chunkCid, bytes: chunkBytes }
            appendBlocks.push(block)
            appendStartOffsets.set(adjust(lastOffset), chunkCid)
            writeUInt(appendIndexBuffer, pos, adjust(lastOffset))
            appendIndexBuffer.set(chunkCid.bytes, pos + 4)
            lastOffset = offset
            pos += blockSize
        }
        appendIndexBuffer.set(origIndexBuffer.subarray(0, origIndexBuffer.length - blockSize), 0)
        writeControlFlag(appendIndexBuffer, 0, INDEX_CONTROL_FLAG) // index control
        writeUInt(appendIndexBuffer, 4, appendIndexSize)  // index size
        writeUInt(appendIndexBuffer, 8, appendByteArraySize)  // byte array size
        const appendRoot = await encode(appendIndexBuffer)
        if (appendRoot.byteLength !== 36) throw new Error(`The cid returned by 'encode' function has unexpected size ${appendIndexBuffer.byteLength}, Expected 36 bytes.`)

        // TODO chunk index on size threshold, fixed size chunks
        const appendRootBlock = { cid: appendRoot, bytes: appendIndexBuffer }
        appendBlocks.push(appendRootBlock)

        return { root: appendRoot, index: appendIndex, blocks: appendBlocks }
    }



    // expected format |<-- index control (4 bytes) -->|<-- index size (4 bytes) -->|<-- byte array size (4 bytes) -->|<-- chunk start offset (4 bytes) -->|<-- chunk CID (36 bytes) -->|...

    const readIndex = async (root: any, get: (root: any) => Promise<Uint8Array>, decode: (bytes: Uint8Array) => any): Promise<{ indexStruct: { startOffsets: Map<number, any>, indexSize: number, byteArraySize: number }, indexBuffer: Uint8Array }> => {
        const indexBuffer = await get(root)
        const controlFlag: number = readControlFlag(indexBuffer, 0)
        if ((controlFlag & INDEX_CONTROL_FLAG) === 0) throw new Error(`This byte array is not an index`)
        const indexSize = readUInt(indexBuffer, 4)
        const byteArraySize = readUInt(indexBuffer, 8)
        const blockSize = INDEX_BLOCK_SIZE
        const shift = INDEX_HEADER_SIZE
        let pos = shift
        const startOffsets = new Map()
        const index = { startOffsets, indexSize, byteArraySize }
        for (let i = 0; i < indexSize; i++) {
            const startOffset = readUInt(indexBuffer, pos)
            const cidBytes = readBytes(indexBuffer, pos + 4, 36)
            const chunkCid = decode(cidBytes)
            startOffsets.set(startOffset, chunkCid)
            pos += blockSize
        }

        return { indexStruct: index, indexBuffer }
    }

    return { create, read, append }
}

export { chunkyStore }




