import { cpus } from "os";
import { compress } from "lz4-napi";

const processorCount = cpus().length;
const concurrency = Math.max(processorCount - 1, 1);

const RawBlockSize = 60 * 1024;

type CompressJob = {
    data: Buffer,
    resolve: (value: Buffer) => void,
    reject: (reason?: any) => void,
}

const pendingJobs: CompressJob[] = [];
let running: number = 0;

export class Lz4Compress {
    static compress(data: Buffer): Promise<Buffer[]> {
        return data.length < RawBlockSize
               ? Lz4Compress.compressBlock(data)
               : Lz4Compress.compressBlockArray(data);
    }

    private static async compressBlock(data: Buffer): Promise<Buffer[]> {
        const uncompressedSize = data.length;
        const compressedSizePrepended = await Lz4Compress.compressInternal(data);
        const compressed = compressedSizePrepended.subarray(4);

        const extension = Buffer.allocUnsafe(12);
        let extSize = Lz4Compress.writeExtensionHeader(extension, compressed.length + 5, 99);
        Lz4Compress.writeInt32(extension.subarray(extSize), uncompressedSize);
        extSize += 5;

        return [extension.subarray(0, extSize), compressed];
    }

    private static async compressBlockArray(data: Buffer): Promise<Buffer[]> {
        // Write to [Ext(98:int,int...), bin,bin,bin...]
        let sequenceCount = 0;
        let HeaderSize = 0;

        const rawBuffers: Buffer[] = [];
        let length = data.length;
        let offset = 0;
        while (length > 0) {
            sequenceCount++;
            const blockSize = Math.min(length, RawBlockSize);
            HeaderSize += Lz4Compress.getUInt32WriteSize(blockSize);

            rawBuffers.push(data.subarray(offset, offset + blockSize));

            offset += blockSize;
            length -= blockSize;
        }

        const extension = Buffer.allocUnsafe(12 + sequenceCount * 6);

        let extSize = Lz4Compress.writeArrayHeader(extension, sequenceCount + 1);
        extSize += Lz4Compress.writeExtensionHeader(extension.subarray(extSize), HeaderSize, 98);
        rawBuffers.forEach(buf => {
            extSize += Lz4Compress.write(extension.subarray(extSize), buf.length);
        });

        const list: Buffer[] = [];
        list.push(extension.subarray(0, extSize));

        for (let i = 0, len = rawBuffers.length; i < len; i++) {
            const buf = rawBuffers[i];
            const compressedSizePrepended = await Lz4Compress.compressInternal(buf);
            const lz4Length = compressedSizePrepended.length - 4;

            extension[extSize] = 0xc6;
            list.push(extension.subarray(extSize, extSize + 1));
            extSize++;

            compressedSizePrepended[0] = (lz4Length >> 24) & 0xFF;
            compressedSizePrepended[1] = (lz4Length >> 16) & 0xFF;
            compressedSizePrepended[2] = (lz4Length >> 8) & 0xFF;
            compressedSizePrepended[3] = lz4Length & 0xFF;
            
            list.push(compressedSizePrepended);
        }

        return list;
    }

    private static getUInt32WriteSize(value: number) {
        if (value <= 127) {
            return 1;
        } else if (value <= 255) {
            return 2;
        } else if (value <= 65535) {
            return 3;
        } else {
            return 5;
        }
    }

    private static write(destination: Buffer, value: number) {
        if (value <= 127) {
            destination[0] = value;
            return 1;
        } else if (value <= 255) {
            destination[0] = 0xcc;
            destination[1] = value;
            return 2;
        } else if (value <= 65535) {
            destination[0] = 0xcd;
            destination[1] = (value >> 8) & 0xFF;
            destination[2] = value & 0xFF;
            return 3;
        } else {
            destination[0] = 0xce;
            destination[1] = (value >> 24) & 0xFF;
            destination[2] = (value >> 16) & 0xFF;
            destination[3] = (value >> 8) & 0xFF;
            destination[4] = value & 0xFF;
            return 5;
        }
    }

    private static writeArrayHeader(destination: Buffer, arraySize: number): number {
        if (arraySize <= 15) {
            destination[0] = (0x90 | arraySize);
            return 1;
        } else if (arraySize <= 65535) {
            destination[0] = 0xdc;
            destination[1] = (arraySize >> 8) & 0xFF;
            destination[2] = arraySize & 0xFF;
            return 3;
        } else {
            destination[0] = 0xdd;
            destination[1] = (arraySize >> 24) & 0xFF;
            destination[2] = (arraySize >> 16) & 0xFF;
            destination[3] = (arraySize >> 8) & 0xFF;
            destination[4] = arraySize & 0xFF;
            return 5;
        }
    }

    private static writeExtensionHeader(destination: Buffer, dataLength: number, typeCode: number): number {
        if (dataLength <= 0xFFFF) {
            destination[0] = 0xc8;
            destination[1] = (dataLength >> 8) & 0xFF;
            destination[2] = dataLength & 0xFF;
            destination[3] = typeCode;
            return 4;
        } else {
            destination[0] = 0xc9;
            destination[1] = (dataLength >> 24) & 0xFF;
            destination[2] = (dataLength >> 16) & 0xFF;
            destination[3] = (dataLength >> 8) & 0xFF;
            destination[4] = dataLength & 0xFF;
            destination[5] = typeCode;
            return 6;
        }
    }

    private static writeInt32(destination: Buffer, value: number) {
        destination[0] = 0xd2;
        destination[1] = (value >> 24) & 0xFF;
        destination[2] = (value >> 16) & 0xFF;
        destination[3] = (value >> 8) & 0xFF;
        destination[4] = value & 0xFF;
    }

    // Internal methods for lz4 compression
    private static compressInternal(data: Buffer): Promise<Buffer> {
        return new Promise<Buffer>((resolve, reject) => {
            pendingJobs.push({
                data,
                resolve,
                reject,
            })

            Lz4Compress.runJob();
        });
    }

    private static async runJob() {
        if (running === concurrency) return;

        if (pendingJobs.length) {
            running++;

            let job: CompressJob;
            while (job = pendingJobs.shift()) {
                try {
                    const compressed = await compress(job.data);
                    job.resolve(compressed);
                } catch (e) {
                    job.reject(e);
                }
            }

            running--;
        }
    }
}