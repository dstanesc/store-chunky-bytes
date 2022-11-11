import bounds from 'binary-search-bounds'

// const INDEX_CONTROL_FLAG: number = 0b100100 // Dec 36, Hex 0x24 // absolute offset structure

const INDEX_CONTROL_FLAG: number = 0b111100 // Dec 60, Hex 0x3C // relative offsets

const INDEX_HEADER_SIZE: number = 12 // bytes |<-- index control (4 bytes) -->|<-- index size (4 bytes) -->|<-- byte array size (4 bytes) -->|

const INDEX_BLOCK_SIZE: number = 40 // bytes |<-- chunk relative offset (4 bytes) -->|<-- chunk CID (36 bytes) -->|

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
     *  Create an array of binary blocks by chunking the input buffer. The blocks are content identified.
     *
     *  @param {Uint8Array} buff - Input buffer to partition in (content defined) chunks
     *  @param {(data: Uint8Array) => Uint32Array} chunk - Chunking algorithm to apply on the input. Should return a list of chunk start offsets
     *  @param {(chunkBytes: Uint8Array) => Promise<any> } encode - Cid encoding function
     *  
     *  @returns {{any, any,  {cid: any, bytes: Uint8Array }[]} } root, index, blocks - A data structure containing the chunks (to persist) and the root handle
     */
    const create = async ({ buf, chunk, encode }: { buf: Uint8Array, chunk: (data: Uint8Array) => Uint32Array, encode: (chunkBytes: Uint8Array) => Promise<any> }): Promise<{ root: any, index: { indexStruct: { startOffsets: Map<number, any>, indexSize: number, byteArraySize: number }, indexBuffer: Uint8Array }, blocks: { cid: any, bytes: Uint8Array }[] }> => {
        const offsets = chunk(buf)
        const shift = INDEX_HEADER_SIZE // allow index header
        const blockSize = INDEX_BLOCK_SIZE
        let beforeLastOffset = 0
        let lastOffset = 0
        let pos = shift
        const startOffsets: Map<number, any> = new Map()
        const blocks: { cid: any, bytes: Uint8Array }[] = [] // {cid, bytes}
        const indexSize: number = offsets.length
        const byteArraySize: number = buf.length
        const indexStruct: { startOffsets: Map<number, any>, indexSize: number, byteArraySize: number } = { startOffsets /*, endOffsets*/, indexSize: indexSize, byteArraySize: byteArraySize }
        const indexBuffer = new Uint8Array(indexSize * blockSize + shift)
        const index = { indexStruct, indexBuffer }
        for (const offset of offsets.values()) {
            const chunkBytes = buf.subarray(lastOffset, offset)
            const chunkCid = await encode(chunkBytes)
            if (chunkCid.byteLength !== CHUNK_CONTENT_IDENTIFIER_SIZE) throw new Error(`The cid returned by 'encode' function has unexpected size ${chunkCid.byteLength}. Expected 36 bytes.`)
            const block = { cid: chunkCid, bytes: chunkBytes }
            blocks.push(block)
            startOffsets.set(lastOffset, chunkCid)
            writeUInt(indexBuffer, pos, lastOffset - beforeLastOffset)
            //console.log(`Writing @ ${lastOffset - beforeLastOffset} - ${chunkCid}`)
            indexBuffer.set(chunkCid.bytes, pos + 4)
            beforeLastOffset = lastOffset
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


    const relevantChunks = (startOffsetArray: any[], startOffset: number, endOffset: number, pad: number): any[] => {

        return startOffsetArray.slice(bounds.le(startOffsetArray, startOffset), bounds.ge(startOffsetArray, endOffset) + pad)
    }

    /*
     *  Read a byte array slice
     *
     *  @param {number} startOffset - Zero-based valid index at which to begin extraction.
     *  @param {number} length - Number of bytes to read
     *  @param {any} root - The root content identifier as returned by the `create` function
     *  @param {{ startOffsets: Map<number, any>, indexSize: number, byteArraySize: number }} index (optional) - the cached index as returned by the `create` function
     *  @param {(cidBytes: Uint8Array) => any} decode - Cid decoding function
     *  @param {(cid: any) => Promise<Uint8Array> }} get - data block access function
     *  @returns {Uint8Array} - Requested slice of data
     */
    const read = async (startOffset: number, length: number, { root, index, decode, get }: { root?: any, index?: any, decode: (cidBytes: Uint8Array) => any, get: (cid: any) => Promise<Uint8Array> }): Promise<Uint8Array> => {
        if (index === undefined) {
            if (root === undefined) throw new Error(`Missing root, please provide an index or root as arg`)
            index = await readIndex(root, get, decode)
        }
        const { indexStruct } = index
        const endOffset = startOffset + length
        if (startOffset > indexStruct.byteArraySize) throw new Error(`Start offset out of range ${startOffset} > buffer size ${indexStruct.byteArraySize}`)
        if (endOffset > indexStruct.byteArraySize) throw new Error(`End offset out of range ${endOffset} > buffer size ${indexStruct.byteArraySize}`)
        const startOffsetsIndexed = indexStruct.startOffsets
        const startOffsetArray = Array.from(startOffsetsIndexed.keys())
        const selectedChunks = relevantChunks(startOffsetArray, startOffset, endOffset, 1)
        const resultBuffer: Uint8Array = new Uint8Array(length)
        let cursor = 0
        for (let i = 0; i < selectedChunks.length; i++) {
            const chunkOffset = selectedChunks[i]
            const chunkCid = startOffsetsIndexed.get(chunkOffset)
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

        if (cursor !== resultBuffer.byteLength) throw new Error(`alg. error, check code cursor=${cursor}, resultBuffer=${resultBuffer.byteLength}`)

        return resultBuffer
    }

    /*
     *  Read all blocks, ie. full byte array
     *
     *  @param {any} root - The root content identifier as returned by the `create` function
     *  @param {{ startOffsets: Map<number, any>, indexSize: number, byteArraySize: number }} index (optional) - the cached index as returned by the `create` function
     *  @param {(cidBytes: Uint8Array) => any} decode - Cid decoding function
     *  @param {(cid: any) => Promise<Uint8Array> }} get - data block access function
     *  @returns {Uint8Array} - Requested slice of data
     */
    const readAll = async ({ root, index, decode, get }: { root?: any, index?: any, decode: (cidBytes: Uint8Array) => any, get: (cid: any) => Promise<Uint8Array> }, debugCallback?: Function): Promise<Uint8Array> => {
        if (index === undefined) {
            if (root === undefined) throw new Error(`Missing root, please provide an index or root as arg`)
            index = await readIndex(root, get, decode)
        }
        return await read(0, index.indexStruct.byteArraySize, { root, index, decode, get })
    }

    /*
     *  Append the input byte array to an existing byte array. The behavior is correct only if:
     *
     *  1. the same chunking algorithm is used as in the original `create`
     *  2. the chunking algorithm is content-defined
     * 
     *  If above conditions are met, the function will return the incremental blocks and a new root. 
     *  The new root can be used to read any slice of data from the combined byte arrays.
     *  
     *  @param {any} root - The root content identifier of a previous chunked array as returned by the `create`, `update` or `append` function
     *  @param index - Optional offset lookup index as returned by the `create`, `update` or `append` function
     *  @param {(cidBytes: Uint8Array) => any} decode - Cid decoding function
     *  @param {(cid: any) => Promise<Uint8Array> }} get - data block access function
     *  @param {Uint8Array} buff - Input buffer to partition in (content defined) chunks
     *  @param {(data: Uint8Array) => Uint32Array} chunk - Chunking algorithm to apply on the input. Should return a list of chunk start offsets
     *  @param {(chunkBytes: Uint8Array) => Promise<any> } encode - Cid encoding function
     *  
     *  @returns {{any, any,  {cid: any, bytes: Uint8Array }[]} } root, index, blocks - A data structure containing the chunks (to persist) and the root handle
     */
    const append = async ({ root, index, decode, get }: { root?: any, index?: any, decode: (cidBytes: Uint8Array) => any, get: (cid: any) => Promise<Uint8Array> }, { buf, chunk, encode }: { buf: Uint8Array, chunk: (data: Uint8Array) => Uint32Array, encode: (chunkBytes: Uint8Array) => Promise<any> }): Promise<{ root: any, index: { indexStruct: { startOffsets: Map<number, any>, indexSize: number, byteArraySize: number }, indexBuffer: Uint8Array }, blocks: { cid: any, bytes: Uint8Array }[] }> => {

        if (index === undefined) {
            if (root === undefined) throw new Error(`Missing root, please provide the index or root as arg`)
            index = await readIndex(root, get, decode)
        }

        const { indexStruct: origIndex, indexBuffer: origIndexBuffer } = index
        const { startOffsets: origStartOffsets, indexSize: origIndexSize, byteArraySize: origByteArraySize } = origIndex
        const origStartOffsetArray: number[] = Array.from(origStartOffsets.keys())

        if (origByteArraySize === 0) {
            // if empty original, create new 
            return await create({ buf, chunk, encode })
        }

        const lastChunkOffset = origStartOffsetArray[origStartOffsetArray.length - 1]
        const lastChunkCid = origStartOffsets.get(lastChunkOffset)
        const lastChunkBuffer = await get(lastChunkCid)
        let beforeLastAbsoluteOffset = origStartOffsetArray.length > 1 ? origStartOffsetArray[origStartOffsetArray.length - 2] : 0

        const adjust = (offset: number): number => offset + lastChunkOffset
        const overlapBuffer = new Uint8Array(lastChunkBuffer.byteLength + buf.byteLength)
        overlapBuffer.set(lastChunkBuffer, 0)
        overlapBuffer.set(buf, lastChunkBuffer.byteLength)
        const appendOffsets = chunk(overlapBuffer)
        const appendBlocks: { cid: any, bytes: Uint8Array }[] = [] // {cid, bytes}
        let lastAbsoluteAppendOffset = 0
        const shift = INDEX_HEADER_SIZE
        const blockSize = INDEX_BLOCK_SIZE
        let pos = (origIndexSize - 1) * blockSize + shift
        const appendStartOffsets: Map<number, any> = new Map(origStartOffsets)
        const appendIndexSize: number = origIndexSize + appendOffsets.length - 1
        const appendByteArraySize: number = origByteArraySize + buf.length
        const appendIndexStruct: { startOffsets: Map<number, any>, indexSize: number, byteArraySize: number } = { startOffsets: appendStartOffsets /*, endOffsets*/, indexSize: appendIndexSize, byteArraySize: appendByteArraySize }
        const appendIndexBuffer = new Uint8Array(appendIndexSize * blockSize + shift)
        const appendIndex = { indexStruct: appendIndexStruct, indexBuffer: appendIndexBuffer }
        for (const absoluteAppendOffset of appendOffsets.values()) {
            const chunkBytes = overlapBuffer.subarray(lastAbsoluteAppendOffset, absoluteAppendOffset)
            const chunkCid = await encode(chunkBytes)
            if (chunkCid.byteLength !== CHUNK_CONTENT_IDENTIFIER_SIZE) throw new Error(`The cid returned by 'encode' function has unexpected size ${chunkCid.byteLength}. Expected 36 bytes.`)
            const block = { cid: chunkCid, bytes: chunkBytes }
            appendBlocks.push(block)
            appendStartOffsets.set(adjust(lastAbsoluteAppendOffset), chunkCid)
            writeUInt(appendIndexBuffer, pos, adjust(lastAbsoluteAppendOffset) - beforeLastAbsoluteOffset)
            //console.log(`Writing @ relative=${adjust(lastAbsoluteAppendOffset) - beforeLastAbsoluteOffset} absolute=${adjust(lastAbsoluteAppendOffset)}- ${chunkCid}`)
            appendIndexBuffer.set(chunkCid.bytes, pos + 4)
            beforeLastAbsoluteOffset = adjust(lastAbsoluteAppendOffset)
            lastAbsoluteAppendOffset = absoluteAppendOffset
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

    /*
     *  Update an existing byte array based on the supplied buffer. The behavior is correct only if:
     *
     *  1. the same chunking algorithm is used as in the original `create`
     *  2. the chunking algorithm is content-defined
     * 
     *  If above conditions are met, the function will return the incremental blocks and a new root. 
     *  The new root can be used to read any slice of data from the resulting byte array.
     *  
     *  @param {any} root - The root content identifier of a previous chunked array as returned by the `create`, `update` or `append` function
     *  @param index - Optional offset lookup index as returned by the `create`, `update` or `append` function
     *  @param {(cidBytes: Uint8Array) => any} decode - Cid decoding function
     *  @param {(cid: any) => Promise<Uint8Array> }} get - data block access function
     *  @param {Uint8Array} buff - Input buffer for the update
     *  @param {(data: Uint8Array) => Uint32Array} chunk - Chunking algorithm to apply on the input. Should return a list of chunk start offsets
     *  @param {(chunkBytes: Uint8Array) => Promise<any> } encode - Cid encoding function
     *  @param {number} startOffset - The offset to apply changes from
     * 
     *  @returns {{any, any,  {cid: any, bytes: Uint8Array }[]} } root, index, blocks - A data structure containing the chunks (to persist) and the root handle
     */
    const update = async ({ root, index, decode, get }: { root?: any, index?: any, decode: (cidBytes: Uint8Array) => any, get: (cid: any) => Promise<Uint8Array> }, { buf, chunk, encode }: { buf: Uint8Array, chunk: (data: Uint8Array) => Uint32Array, encode: (chunkBytes: Uint8Array) => Promise<any> }, startOffset: number): Promise<{ root: any, index: { indexStruct: { startOffsets: Map<number, any>, indexSize: number, byteArraySize: number }, indexBuffer: Uint8Array }, blocks: { cid: any, bytes: Uint8Array }[] }> => {
        if (index === undefined) {
            if (root === undefined) throw new Error(`Missing root, please provide the index or root as arg`)
            index = await readIndex(root, get, decode)
        }
        const { indexStruct: origIndexStruct, indexBuffer: origIndexBuffer } = index
        const { startOffsets: origStartOffsets, indexSize: origIndexSize, byteArraySize: origByteArraySize } = origIndexStruct
        const endOffset = startOffset + buf.byteLength

        if (buf.byteLength === 0) {
            return { root, index, blocks: [] }
        }

        if (startOffset > origIndexStruct.byteArraySize) throw new Error(`Start offset out of range ${startOffset} > buffer size ${origIndexStruct.byteArraySize}`)
        if (endOffset > origIndexStruct.byteArraySize) throw new Error(`End offset out of range ${endOffset} > buffer size ${origIndexStruct.byteArraySize}`)

        // select relevant chunks
        const origStartOffsetArray: number[] = Array.from(origStartOffsets.keys())
        const selectedChunks = relevantChunks(origStartOffsetArray, startOffset, endOffset, 0)
        const firstChunkOffset = selectedChunks[0]
        const lastChunkOffset = selectedChunks[selectedChunks.length - 1]
        const lastChunkOffsetIndex = origStartOffsetArray.indexOf(lastChunkOffset)
        const firstChunkOffsetIndex = origStartOffsetArray.indexOf(firstChunkOffset)

        // padding offsets to maintain re-chunking determinism
        let rightPadding: number
        let rightPaddingOffsets = []
        if (lastChunkOffsetIndex === origStartOffsetArray.length - 1) {
            // if last chunk, do not apply additional padding
            rightPadding = origByteArraySize - lastChunkOffset
        } else if (lastChunkOffsetIndex === origStartOffsetArray.length - 2) {
            // if before last, apply remaining chunk
            rightPadding = origStartOffsetArray[lastChunkOffsetIndex + 1] - lastChunkOffset
            rightPaddingOffsets.push(lastChunkOffset)
        } else if (lastChunkOffsetIndex === origStartOffsetArray.length - 3) {
            // if 2 remaining, apply both
            rightPadding = origStartOffsetArray[lastChunkOffsetIndex + 2] - lastChunkOffset
            rightPaddingOffsets.push(lastChunkOffset)
            rightPaddingOffsets.push(origStartOffsetArray[lastChunkOffsetIndex + 1])
        } else if (lastChunkOffsetIndex <= origStartOffsetArray.length - 4) {
            // all other apply padding of 3 chunks
            rightPadding = origStartOffsetArray[lastChunkOffsetIndex + 3] - lastChunkOffset
            rightPaddingOffsets.push(lastChunkOffset)
            rightPaddingOffsets.push(origStartOffsetArray[lastChunkOffsetIndex + 1])
            rightPaddingOffsets.push(origStartOffsetArray[lastChunkOffsetIndex + 2])
        }

        // target buffer to merge existing bytes & updates
        const targetBuffer: Uint8Array = new Uint8Array((lastChunkOffset - firstChunkOffset) + rightPadding)
        const inputBufferCursor = (cursor: number): number => cursor - startOffset
        const targetBufferCursor = (chunkOffset: number): number => chunkOffset - firstChunkOffset

        let absoluteInputBufferCursor = startOffset
        for (let i = 0; i < selectedChunks.length; i++) {
            const chunkOffset = selectedChunks[i]
            const origChunkCid = origStartOffsets.get(chunkOffset)
            const chunkBuffer = await get(origChunkCid)
            if (chunkOffset <= startOffset && endOffset < chunkOffset + chunkBuffer.byteLength) {
                // single block write full buffer
                chunkBuffer.set(buf, startOffset - chunkOffset)
                targetBuffer.set(chunkBuffer, targetBufferCursor(chunkOffset))
                absoluteInputBufferCursor += endOffset - startOffset
                break
            } else if (chunkOffset <= startOffset) {
                // write first block 
                const bufCursor = inputBufferCursor(absoluteInputBufferCursor)
                const bufSlice = buf.subarray(bufCursor, chunkBuffer.byteLength + chunkOffset - startOffset)
                chunkBuffer.set(bufSlice, startOffset - chunkOffset)
                targetBuffer.set(chunkBuffer, targetBufferCursor(chunkOffset))
                absoluteInputBufferCursor += chunkBuffer.byteLength - (startOffset - chunkOffset)
            } else if (chunkOffset > startOffset && endOffset > chunkOffset + chunkBuffer.byteLength) {
                // write full block
                const bufCursor = inputBufferCursor(absoluteInputBufferCursor)
                const bufSlice = buf.subarray(bufCursor, bufCursor + chunkBuffer.byteLength)
                chunkBuffer.set(bufSlice, 0)
                targetBuffer.set(chunkBuffer, targetBufferCursor(chunkOffset))
                absoluteInputBufferCursor += chunkBuffer.byteLength
            } else if (chunkOffset > startOffset && endOffset <= chunkOffset + chunkBuffer.byteLength) {
                // write last block
                const bufCursor = inputBufferCursor(absoluteInputBufferCursor)
                const bufSlice = buf.subarray(bufCursor, buf.length - 1)
                chunkBuffer.set(bufSlice, 0)
                targetBuffer.set(chunkBuffer, targetBufferCursor(chunkOffset))
                absoluteInputBufferCursor += endOffset - chunkOffset
                break
            }
        }

        // apply padding to the right
        for (let i = 0; i < rightPaddingOffsets.length; i++) {
            const paddingOffset = rightPaddingOffsets[i]
            const paddingCid = origStartOffsets.get(paddingOffset)
            const paddingBuffer = await get(paddingCid)
            targetBuffer.set(paddingBuffer, targetBufferCursor(paddingOffset))
        }

        const shift = INDEX_HEADER_SIZE
        const blockSize = INDEX_BLOCK_SIZE
        const updateStartOffsets: Map<number, any> = new Map()
        const updateByteArraySize: number = origByteArraySize
        const updateIndexStruct: { startOffsets: Map<number, any>, indexSize: number, byteArraySize: number } = { startOffsets: updateStartOffsets, indexSize: undefined, byteArraySize: updateByteArraySize }
        const updateIndexBuffer = new Uint8Array((origIndexSize * blockSize + shift) * 2) // 2x index buffer as more chunks may be generated during update re-chunking
        const updateIndex = { indexStruct: updateIndexStruct, indexBuffer: updateIndexBuffer }

        // re-chunk a large enough buffer to fit existing cdc boundaries
        const updateOffsets = chunk(targetBuffer)

        const updateBlocks: { cid: any, bytes: Uint8Array }[] = [] // {cid, bytes}

        let checksum = 0
        let beforePrevOffset = 0
        let prevOffset = 0
        let pos = shift
        let indexCursor = 0

        // reuse chunks before change
        for (let i = 0; i < firstChunkOffsetIndex; i++) {
            indexCursor = i
            const chunkOffset = origStartOffsetArray[i]
            if (chunkOffset < firstChunkOffset) {
                const chunkCid = origStartOffsets.get(chunkOffset)
                updateStartOffsets.set(chunkOffset, chunkCid)
                const relativeOffset = chunkOffset - prevOffset
                writeUInt(updateIndexBuffer, pos, chunkOffset - prevOffset)
                updateIndexBuffer.set(chunkCid.bytes, pos + 4)
                beforePrevOffset = prevOffset
                prevOffset = chunkOffset
                pos += blockSize
                checksum += relativeOffset
            } else break
        }

        // encode new chunks
        beforePrevOffset = firstChunkOffsetIndex > 0 ? origStartOffsetArray[firstChunkOffsetIndex - 1] : 0
        prevOffset = firstChunkOffset
        for (const updateOffset of updateOffsets.values()) {
            const chunkBytes = targetBuffer.subarray(targetBufferCursor(prevOffset), updateOffset)
            const chunkCid = await encode(chunkBytes)
            if (chunkCid.byteLength !== CHUNK_CONTENT_IDENTIFIER_SIZE) throw new Error(`The cid returned by 'encode' function has unexpected size ${chunkCid.byteLength}. Expected 36 bytes.`)
            updateStartOffsets.set(prevOffset, chunkCid)
            const block = { cid: chunkCid, bytes: chunkBytes }
            updateBlocks.push(block)
            const relativeOffset = prevOffset - beforePrevOffset
            writeUInt(updateIndexBuffer, pos, relativeOffset)
            updateIndexBuffer.set(chunkCid.bytes, pos + 4)
            beforePrevOffset = prevOffset
            prevOffset = updateOffset + firstChunkOffset
            pos += blockSize
            checksum += relativeOffset
        }

        // reuse chunks after change
        const boundary = prevOffset
        prevOffset = beforePrevOffset
        for (let i = indexCursor; i < origStartOffsetArray.length; i++) {
            const chunkOffset = origStartOffsetArray[i]
            if (chunkOffset >= boundary) {
                const chunkCid = origStartOffsets.get(chunkOffset)
                updateStartOffsets.set(chunkOffset, chunkCid)
                const relativeOffset = chunkOffset - prevOffset
                writeUInt(updateIndexBuffer, pos, relativeOffset)
                updateIndexBuffer.set(chunkCid.bytes, pos + 4)
                prevOffset = chunkOffset
                pos += blockSize
                checksum += relativeOffset
            }
        }

        // compute index size
        const updateIndexSize = (pos - shift) / blockSize

        // update unknown data at indexStruct creation time

        updateIndex.indexStruct.indexSize = updateIndexSize
        // index header
        writeControlFlag(updateIndexBuffer, 0, INDEX_CONTROL_FLAG) // index control
        writeUInt(updateIndexBuffer, 4, updateIndexSize)  // index size
        writeUInt(updateIndexBuffer, 8, updateByteArraySize)  // byte array size

        checksum += updateByteArraySize - prevOffset

        // validate checksum
        if (checksum !== updateByteArraySize) throw new Error(`Invalid checksum. Error in chunk & merge algorithm checksum+${checksum} != ${updateByteArraySize}`)

        // trim unused buffer 
        const finalIndexBuffer = updateIndexBuffer.subarray(0, pos) // trim space

        const updateRoot = await encode(finalIndexBuffer)
        if (updateRoot.byteLength !== 36) throw new Error(`The cid returned by 'encode' function has unexpected size ${finalIndexBuffer.byteLength}, Expected 36 bytes.`)

        // TODO chunk index on size threshold, fixed size chunks
        const updateRootBlock = { cid: updateRoot, bytes: finalIndexBuffer }
        updateBlocks.push(updateRootBlock)

        return { root: updateRoot, index: updateIndex, blocks: updateBlocks }
    }

    /**
     * Bulk operation combining multiple updates in a single roundtrip
     * 
     *  @param {any} root - The root content identifier of a previous chunked array as returned by the `create`, `update` or `append` function
     *  @param index - Optional offset lookup index as returned by the `create`, `update` or `append` function
     *  @param {(cidBytes: Uint8Array) => any} decode - Cid decoding function
     *  @param {(cid: any) => Promise<Uint8Array> }} get - data block access function
     *  @param {(data: Uint8Array) => Uint32Array} chunk - Chunking algorithm to apply on the input. Should return a list of chunk start offsets
     *  @param {(chunkBytes: Uint8Array) => Promise<any> } encode - Cid encoding function
     *  @param {Uint8Array} updateBuffer - Input buffer for the update
     *  @param {number} updateStartOffset - The offset to apply changes from
     *
     */
    const bulkUpdate = async ({ root, index, decode, get }: { root?: any, index?: any, decode: (cidBytes: Uint8Array) => any, get: (cid: any) => Promise<Uint8Array> }, { chunk, encode }: { chunk: (data: Uint8Array) => Uint32Array, encode: (chunkBytes: Uint8Array) => Promise<any> }, updates: { updateBuffer: Uint8Array, updateStartOffset: number } []): Promise<{ root: any, index: { indexStruct: { startOffsets: Map<number, any>, indexSize: number, byteArraySize: number }, indexBuffer: Uint8Array }, blocks: { cid: any, bytes: Uint8Array }[] }> => {
        let updateRoot =  root 
        let updateIndex = index
        const blocks = []
        for (let i = 0; i < updates.length; i++) {
            const { updateBuffer, updateStartOffset } = updates[i]
            const { root: tempRoot, index: tempIndex, blocks: tempBlocks } = await update({ root: updateRoot, index: updateIndex, decode, get }, { buf: updateBuffer, chunk, encode }, updateStartOffset)  
            updateRoot = tempRoot
            updateIndex = tempIndex
            blocks.push(...tempBlocks)
        }
        return {root: updateRoot, index: updateIndex, blocks }
    }


    /**
     * Bulk operation combining `append` and `bulkUpdate` in a single roundtrip
     * 
     *  @param {any} root - The root content identifier of a previous chunked array as returned by the `create`, `update` or `append` function
     *  @param index - Optional offset lookup index as returned by the `create`, `update` or `append` function
     *  @param {(cidBytes: Uint8Array) => any} decode - Cid decoding function
     *  @param {(cid: any) => Promise<Uint8Array> }} get - data block access function
     *  @param {(data: Uint8Array) => Uint32Array} chunk - Chunking algorithm to apply on the input. Should return a list of chunk start offsets
     *  @param {(chunkBytes: Uint8Array) => Promise<any> } encode - Cid encoding function
     *  @param {Uint8Array} appendBuffer - Input buffer for the append
     *  @param {Uint8Array} updateBuffer - Input buffer for the update
     *  @param {number} updateStartOffset - The offset to apply changes from
     *
     */
    const bulk = async ({ root, index, decode, get }: { root?: any, index?: any, decode: (cidBytes: Uint8Array) => any, get: (cid: any) => Promise<Uint8Array> }, { chunk, encode }: { chunk: (data: Uint8Array) => Uint32Array, encode: (chunkBytes: Uint8Array) => Promise<any> }, appendBuffer: Uint8Array, updates: { updateBuffer: Uint8Array, updateStartOffset: number } []): Promise<{ root: any, index: { indexStruct: { startOffsets: Map<number, any>, indexSize: number, byteArraySize: number }, indexBuffer: Uint8Array }, blocks: { cid: any, bytes: Uint8Array }[] }> => {
        const blocks = []
        const { root: appendRoot, index: appendIndex, blocks: appendBlocks } = await append({ root, index, decode, get }, { buf: appendBuffer, chunk, encode })
        const { root: updateRoot, index: updateIndex, blocks: updateBlocks } = await bulkUpdate({ root: appendRoot, index: appendIndex, decode, get }, { chunk, encode }, updates )
        //const blocks = [...appendBlocks, ...updateBlocks]
        blocks.push(...appendBlocks)
        blocks.push(...updateBlocks)
        return { root: updateRoot, index: updateIndex, blocks }
    }


    /**
     *  Remove (delete) a byte array fragment. The behavior is correct only if:
     *
     *  1. the same chunking algorithm is used as in the original `create`
     *  2. the chunking algorithm is content-defined
     * 
     *  If above conditions are met, the function will return the incremental blocks and a new root. 
     *  The new root can be used to read any slice of data from the resulting byte array
     *  
     *  @param {any} root - The rood content identifier of a previous chunked array as returned by the `create` function
     *  @param {(cidBytes: Uint8Array) => any} decode - Cid decoding function
     *  @param {(cid: any) => Promise<Uint8Array> }} get - data block access function
     *  @param {Uint8Array} buff - Input buffer for the update
     *  @param {(data: Uint8Array) => Uint32Array} chunk - Chunking algorithm to apply on the input. Should return a list of chunk start offsets
     *  @param {(chunkBytes: Uint8Array) => Promise<any> } encode - Cid encoding function
     *  @param {number} startOffset - The remove start offset
     *  @param {number} length - The length of the byte array fragment to remove
     * 
     *  @returns {{any, any,  {cid: any, bytes: Uint8Array }[]} } root, index, blocks - A data structure containing the chunks (to persist) and the root handle
     */
    const remove = async ({ root, index, decode, get }: { root?: any, index?: any, decode: (cidBytes: Uint8Array) => any, get: (cid: any) => Promise<Uint8Array> }, { chunk, encode }: { chunk: (data: Uint8Array) => Uint32Array, encode: (chunkBytes: Uint8Array) => Promise<any> }, startOffset: number, length: number): Promise<{ root: any, index: { indexStruct: { startOffsets: Map<number, any>, indexSize: number, byteArraySize: number }, indexBuffer: Uint8Array }, blocks: { cid: any, bytes: Uint8Array }[] }> => {

        if (index === undefined) {
            if (root === undefined) throw new Error(`Missing root, please provide the index or root as arg`)
            index = await readIndex(root, get, decode)
        }
        const { indexStruct: origIndexStruct, indexBuffer: origIndexBuffer } = index
        const { startOffsets: origStartOffsets, indexSize: origIndexSize, byteArraySize: origByteArraySize } = origIndexStruct
        const endOffset = startOffset + length
        const delta = length

        if (startOffset > origIndexStruct.byteArraySize) throw new Error(`Start offset out of range ${startOffset} > buffer size ${origIndexStruct.byteArraySize}`)
        if (endOffset > origIndexStruct.byteArraySize) throw new Error(`End offset out of range ${endOffset} > buffer size ${origIndexStruct.byteArraySize}`)

        // select relevant chunks
        const origStartOffsetArray: number[] = Array.from(origStartOffsets.keys())
        const firstChunkOffset = origStartOffsetArray[bounds.le(origStartOffsetArray, startOffset)]
        const lastChunkOffset = origStartOffsetArray[bounds.le(origStartOffsetArray, endOffset)]
        const lastChunkOffsetIndex = origStartOffsetArray.indexOf(lastChunkOffset)
        const firstChunkOffsetIndex = origStartOffsetArray.indexOf(firstChunkOffset)

        // padding offsets to maintain re-chunking determinism
        let rightPadding: number
        let rightPaddingOffsets = []
        let lastChunkByteLength: number

        if (lastChunkOffsetIndex === origStartOffsetArray.length - 1) {
            lastChunkByteLength = origByteArraySize - lastChunkOffset
            rightPadding = 0
        } else if (lastChunkOffsetIndex === origStartOffsetArray.length - 2) {
            lastChunkByteLength = origStartOffsetArray[lastChunkOffsetIndex + 1] - lastChunkOffset
            rightPadding = origByteArraySize - origStartOffsetArray[lastChunkOffsetIndex + 1]
            rightPaddingOffsets.push(origStartOffsetArray[lastChunkOffsetIndex + 1])
        } else if (lastChunkOffsetIndex === origStartOffsetArray.length - 3) {
            lastChunkByteLength = origStartOffsetArray[lastChunkOffsetIndex + 1] - lastChunkOffset
            rightPadding = origStartOffsetArray[lastChunkOffsetIndex + 2] - origStartOffsetArray[lastChunkOffsetIndex + 1]
            rightPaddingOffsets.push(origStartOffsetArray[lastChunkOffsetIndex + 1])
        } else if (lastChunkOffsetIndex <= origStartOffsetArray.length - 4) {
            lastChunkByteLength = origStartOffsetArray[lastChunkOffsetIndex + 1] - lastChunkOffset
            rightPadding = origStartOffsetArray[lastChunkOffsetIndex + 3] - origStartOffsetArray[lastChunkOffsetIndex + 1]
            rightPaddingOffsets.push(origStartOffsetArray[lastChunkOffsetIndex + 1])
            rightPaddingOffsets.push(origStartOffsetArray[lastChunkOffsetIndex + 2])
        }

        const targetBufferLengthNopad = startOffset - firstChunkOffset + lastChunkOffset + lastChunkByteLength - endOffset
        const targetBuffer: Uint8Array = new Uint8Array(targetBufferLengthNopad + rightPadding)
        const firstChunkCid = origStartOffsets.get(firstChunkOffset)
        const firstChunkBuffer = await get(firstChunkCid)
        const lastChunkCid = origStartOffsets.get(lastChunkOffset)
        const lastChunkBuffer = await get(lastChunkCid)

        let targetBufferPos = 0
        targetBuffer.set(firstChunkBuffer.subarray(0, startOffset - firstChunkOffset), targetBufferPos)
        targetBufferPos += startOffset - firstChunkOffset
        targetBuffer.set(lastChunkBuffer.subarray(endOffset - lastChunkOffset, lastChunkOffset + lastChunkBuffer.byteLength), targetBufferPos)
        targetBufferPos += lastChunkOffset + lastChunkBuffer.byteLength - endOffset

        const targetBufferCursor = (chunkOffset: number): number => chunkOffset - firstChunkOffset

        //apply padding to the right
        let paddingCursor = targetBufferLengthNopad
        for (let i = 0; i < rightPaddingOffsets.length; i++) {
            const paddingOffset = rightPaddingOffsets[i]
            const paddingCid = origStartOffsets.get(paddingOffset)
            const paddingBuffer = await get(paddingCid)
            targetBuffer.set(paddingBuffer, paddingCursor)
            paddingCursor += paddingBuffer.byteLength
        }

        const shift = INDEX_HEADER_SIZE
        const blockSize = INDEX_BLOCK_SIZE
        const updateStartOffsets: Map<number, any> = new Map()
        const removeByteArraySize: number = origByteArraySize - delta
        const updateIndexStruct: { startOffsets: Map<number, any>, indexSize: number, byteArraySize: number } = { startOffsets: updateStartOffsets, indexSize: undefined, byteArraySize: removeByteArraySize }
        const removeIndexBuffer = new Uint8Array((origIndexSize * blockSize + shift)) // 
        const removeIndex = { indexStruct: updateIndexStruct, indexBuffer: removeIndexBuffer }

        // re-chunk a large enough buffer to fit existing cdc boundaries
        const updateOffsets = chunk(targetBuffer)
        const removeBlocks: { cid: any, bytes: Uint8Array }[] = [] // {cid, bytes}

        let checksum = 0
        let beforePrevOffset = 0
        let prevOffset = 0
        let pos = shift
        let indexCursor = 0
        // reuse chunks before change
        for (let i = 0; i < firstChunkOffsetIndex; i++) {
            indexCursor = i
            const chunkOffset = origStartOffsetArray[i]
            if (chunkOffset < firstChunkOffset) {
                const chunkCid = origStartOffsets.get(chunkOffset)
                updateStartOffsets.set(chunkOffset, chunkCid)
                const relativeOffset = chunkOffset - prevOffset
                writeUInt(removeIndexBuffer, pos, chunkOffset - prevOffset)
                removeIndexBuffer.set(chunkCid.bytes, pos + 4)
                beforePrevOffset = prevOffset
                prevOffset = chunkOffset
                pos += blockSize
                checksum += relativeOffset
            } else break
        }

        // encode new chunks
        beforePrevOffset = firstChunkOffsetIndex > 0 ? origStartOffsetArray[firstChunkOffsetIndex - 1] : 0
        prevOffset = firstChunkOffset
        for (const updateOffset of updateOffsets.values()) {
            const chunkBytes = targetBuffer.subarray(targetBufferCursor(prevOffset), updateOffset)
            const chunkCid = await encode(chunkBytes)
            if (chunkCid.byteLength !== CHUNK_CONTENT_IDENTIFIER_SIZE) throw new Error(`The cid returned by 'encode' function has unexpected size ${chunkCid.byteLength}. Expected 36 bytes.`)
            updateStartOffsets.set(prevOffset, chunkCid)
            const block = { cid: chunkCid, bytes: chunkBytes }
            removeBlocks.push(block)
            const relativeOffset = prevOffset - beforePrevOffset
            writeUInt(removeIndexBuffer, pos, relativeOffset)
            removeIndexBuffer.set(chunkCid.bytes, pos + 4)
            beforePrevOffset = prevOffset
            prevOffset = updateOffset + firstChunkOffset
            pos += blockSize
            checksum += relativeOffset
        }


        // reuse chunks after change
        const boundary = firstChunkOffset + targetBuffer.byteLength + delta
        //const boundary = prevOffset
        prevOffset = beforePrevOffset
        for (let i = indexCursor; i < origStartOffsetArray.length; i++) {
            const origChunkOffset = origStartOffsetArray[i]
            if (origChunkOffset >= boundary) {
                const chunkCid = origStartOffsets.get(origChunkOffset)
                const newChunkOffset = origChunkOffset - delta
                updateStartOffsets.set(newChunkOffset, chunkCid)
                const relativeOffset = newChunkOffset - prevOffset
                writeUInt(removeIndexBuffer, pos, relativeOffset)
                removeIndexBuffer.set(chunkCid.bytes, pos + 4)
                prevOffset = newChunkOffset
                pos += blockSize
                checksum += relativeOffset
            }
        }


        // compute index size
        const removeIndexSize = (pos - shift) / blockSize
        removeIndex.indexStruct.indexSize = removeIndexSize

        // index header
        writeControlFlag(removeIndexBuffer, 0, INDEX_CONTROL_FLAG) // index control
        writeUInt(removeIndexBuffer, 4, removeIndexSize)  // index size
        writeUInt(removeIndexBuffer, 8, removeByteArraySize)  // byte array size

        checksum += removeByteArraySize - prevOffset

        // validate checksum
        if (checksum !== removeByteArraySize) throw new Error(`Invalid checksum. Error in chunk & merge algorithm checksum+${checksum} != ${removeByteArraySize}`)

        // trim unused buffer 
        const finalIndexBuffer = removeIndexBuffer.subarray(0, pos) // trim space

        const removeRoot = await encode(finalIndexBuffer)
        if (removeRoot.byteLength !== 36) throw new Error(`The cid returned by 'encode' function has unexpected size ${finalIndexBuffer.byteLength}, Expected 36 bytes.`)

        // TODO chunk index on size threshold, fixed size chunks
        const removeRootBlock = { cid: removeRoot, bytes: finalIndexBuffer }
        removeBlocks.push(removeRootBlock)

        return { root: removeRoot, index: removeIndex, blocks: removeBlocks }
    }


    // expected format |<-- index control (4 bytes) -->|<-- index size (4 bytes) -->|<-- byte array size (4 bytes) -->|<-- chunk relative offset (4 bytes) -->|<-- chunk CID (36 bytes) -->|...

    const readIndex = async (root: any, get: (root: any) => Promise<Uint8Array>, decode: (bytes: Uint8Array) => any): Promise<{ indexStruct: { startOffsets: Map<number, any>, indexSize: number, byteArraySize: number }, indexBuffer: Uint8Array }> => {
        const indexBuffer = await get(root)
        const controlFlag: number = readControlFlag(indexBuffer, 0)
        if ((controlFlag & INDEX_CONTROL_FLAG) === 0) throw new Error(`This byte array is not representing a supported index structure`)
        const indexSize = readUInt(indexBuffer, 4)
        const byteArraySize = readUInt(indexBuffer, 8)
        const blockSize = INDEX_BLOCK_SIZE
        const shift = INDEX_HEADER_SIZE
        let pos = shift
        const startOffsets = new Map()
        let absoluteOffset = 0
        const index = { startOffsets, indexSize, byteArraySize }
        for (let i = 0; i < indexSize; i++) {
            const relativeStartOffset = readUInt(indexBuffer, pos)
            absoluteOffset += relativeStartOffset
            const cidBytes = readBytes(indexBuffer, pos + 4, 36)
            const chunkCid = decode(cidBytes)
            startOffsets.set(absoluteOffset, chunkCid)
            pos += blockSize
        }
        return { indexStruct: index, indexBuffer }
    }

    return { create, read, readAll, append, update, bulk, remove, readIndex }
}

export { chunkyStore }




