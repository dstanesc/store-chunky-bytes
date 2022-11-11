import { CID } from 'multiformats/cid'
import * as raw from 'multiformats/codecs/raw'
import { sha256 } from 'multiformats/hashes/sha2'
import { compute_chunks_buzhash, compute_chunks_fastcdc } from "@dstanesc/wasm-chunking-node-eval";



const codec = () => {
    const encode = async (bytes: Uint8Array): Promise<any> => {
        const chunkHash = await sha256.digest(bytes)
        const chunkCid = CID.create(1, raw.code, chunkHash)
        return chunkCid
    }
    const decode = (cidBytes: Uint8Array): any => {
        return CID.decode(cidBytes)
    }
    return { encode, decode }
}



const blockStore = (other?: any) => {
    const blocks = Object.assign({}, other);
    const put = async (block: { cid: any, bytes: Uint8Array }):  Promise<void> => {
        blocks[block.cid.toString()] = block.bytes
    }
    const get = async (cid: any): Promise<Uint8Array> => {
        const bytes = blocks[cid.toString()]
        if (!bytes) throw new Error('Block not found for ' + cid.toString())
        return bytes
    }
    const size = () => {
        return Object.keys(blocks).length
    }
    return { get, put, size }
}


const chunkerFactory = ({ fastAvgSize, buzMask }: { fastAvgSize?: number, buzMask?: number }) => {

    const FASTCDC_CHUNK_AVG_SIZE_DEFAULT = 32768

    const fastcdc = (buf: Uint8Array) => {

        if (fastAvgSize === undefined)
            fastAvgSize = FASTCDC_CHUNK_AVG_SIZE_DEFAULT

        if (!Number.isInteger(fastAvgSize) || fastAvgSize <= 0) throw new Error('avgSize arg should be a positive integer')

        const minSize = Math.floor(fastAvgSize / 2)
        const maxSize = fastAvgSize * 2

        return compute_chunks_fastcdc(buf, minSize, fastAvgSize, maxSize)
    }

    const BUZHASH_MASK_DEFAULT = 14 //0b11111111111111

    const buzhash = (buf: Uint8Array) => {

        if (buzMask === undefined)
            buzMask = BUZHASH_MASK_DEFAULT

        if (!Number.isInteger(buzMask) || buzMask <= 0) throw new Error('mask arg should be a positive integer')

        return compute_chunks_buzhash(buf, buzMask)
    }

    return { fastcdc, buzhash }
}

export { blockStore, codec, chunkerFactory }