import { cpus } from "os";
import { compress } from "lz4-napi";

const processorCount = cpus().length;
const concurrency = Math.max(processorCount - 1, 1);

const RawBlockSize = 32 * 1024;

type CompressJob = {
    data: Buffer,
    resolve: (value: Buffer) => void,
    reject: (reason?: any) => void,
}

const pendingJobs: CompressJob[] = [];
let running: number = 0;

export class Lz4Compress {
    static async compress(data: Buffer): Promise<Buffer[]> {
        const uncompressedSize = data.length;
        const compressedSizePrepended = await Lz4Compress.compressInternal(data);
        const compressed = compressedSizePrepended.subarray(4);

        const extension = Buffer.allocUnsafe(12);
        const extensionSize = Lz4Compress.writeExtensionHeader(extension, compressed.length + 5, 99);
        Lz4Compress.writeInt32(extension.subarray(extensionSize), uncompressedSize);

        return [extension.subarray(0, extensionSize + 5), compressed];
    }

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

    private static writeExtensionHeader(destination: Buffer, dataLength: number, typeCode: number): number
    {
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

    private static writeInt32(destination: Buffer, value: number)
    {
        destination[0] = 0xd2;
        destination[1] = (value >> 24) & 0xFF;
        destination[2] = (value >> 16) & 0xFF;
        destination[3] = (value >> 8) & 0xFF;
        destination[4] = value & 0xFF;
    }
}