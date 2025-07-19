import EventEmitter from 'events';
import uWebSockets from 'uWebSockets.js';

import { getMessageBytes, Protocol, Client, ClientPrivate, ClientState, ISendOptions, logger, debugMessage } from '@colyseus/core';
import { Lz4Compress } from './Lz4Compress';

export class uWebSocketWrapper extends EventEmitter {
  constructor(public ws: uWebSockets.WebSocket<any>) {
    super();
  }
}

export enum ReadyState {
  CONNECTING = 0,
  OPEN = 1,
  CLOSING = 2,
  CLOSED = 3,
}

export class uWebSocketClient implements Client, ClientPrivate {
  public sessionId: string;
  public state: ClientState = ClientState.JOINING;
  public readyState: number = ReadyState.OPEN;
  public reconnectionToken: string;

  public _enqueuedMessages: any[] = [];
  public _afterNextPatchQueue;
  public _reconnectionToken: string;
  public _joinedAt: number;

  public msgpackLz4: boolean = false;
  public static MinSizeToCompress = 4096;

  constructor(
    public id: string,
    public _ref: uWebSocketWrapper,
  ) {
    this.sessionId = id;

    _ref.on('close', () => this.readyState = ReadyState.CLOSED);
  }

  get ref() { return this._ref; }
  set ref(_ref: uWebSocketWrapper) {
    this._ref = _ref;
    this.readyState = ReadyState.OPEN;
  }

  public sendBytes(type: string | number, bytes: Buffer | Uint8Array, options?: ISendOptions) {
    debugMessage("send bytes(to %s): '%s' -> %j", this.sessionId, type, bytes);

    this.enqueueRaw(
      getMessageBytes.raw(Protocol.ROOM_DATA_BYTES, type, undefined, bytes),
      options,
    );
  }

  public send(messageOrType: any, messageOrOptions?: any | ISendOptions, options?: ISendOptions) {
    debugMessage("send(to %s): '%s' -> %O", this.sessionId, messageOrType, messageOrOptions);

    this.enqueueRaw(
      getMessageBytes.raw(Protocol.ROOM_DATA, messageOrType, messageOrOptions),
      options,
    );
  }

  public enqueueRaw(data: Uint8Array | Buffer, options?: ISendOptions) {
    // use room's afterNextPatch queue
    if (options?.afterNextPatch) {
      this._afterNextPatchQueue.push([this, [data]]);
      return;
    }

    if (this.state === ClientState.JOINING) {
      // sending messages during `onJoin`.
      // - the client-side cannot register "onMessage" callbacks at this point.
      // - enqueue the messages to be send after JOIN_ROOM message has been sent
      // - create a new buffer for enqueued messages, as the underlying buffer might be modified
      this._enqueuedMessages.push(data);
      return;
    }

    this.raw(data, options);
  }

  public raw(data: Uint8Array | Buffer, options?: ISendOptions, cb?: (err?: Error) => void) {
    // skip if client not open
    if (this.readyState !== ReadyState.OPEN) {
      return;
    }

    if (this.msgpackLz4 && data.length >= uWebSocketClient.MinSizeToCompress && data[0] == Protocol.ROOM_DATA) {
      this.rawLz4(data, options, cb);
    } else {
      this._ref.ws.send(data, true, false);
    }
  }

  private async rawLz4(data: Uint8Array | Buffer, options?: ISendOptions, cb?: (err?: Error) => void) {
    let header = 2;
    const prefix = data[1];
    if (prefix >= 0x80 && prefix < 0xc0)
    {
      // fixstr
      header += prefix & 0x1f;
    }
    else if (prefix == 0xd9)
    {
      header += data[2] + 1;
    }
    else if (prefix == 0xda)
    {
      header += data[2] + (data[3] << 8) + 2;
    }
    else if (prefix == 0xdb)
    {
      header += data[2] + (data[3] << 8) + (data[4] << 16) + (data[5] << 24) + 4;
    }
    else
    {
      this._ref.ws.send(data, true, false);
      return;
    }

    const headerData = <Buffer> data.subarray(0, header);

    const uncompressed = <Buffer> data.subarray(header);
    const arrayBuffer = await Lz4Compress.compress(uncompressed);

    if (this.readyState !== ReadyState.OPEN) {
      return;
    }

    const newData = Buffer.concat([headerData, ...arrayBuffer]);

    this._ref.ws.send(newData, true, false);
  }

  public error(code: number, message: string = '', cb?: (err?: Error) => void) {
    this.raw(getMessageBytes[Protocol.ERROR](code, message));

    // delay callback execution - uWS doesn't acknowledge when the message was sent
    // (same API as "ws" transport)
    setTimeout(cb, 1);
  }

  public leave(code?: number, data?: string) {
    if (this.readyState !== ReadyState.OPEN) {
      // connection already closed. ignore.
      return;
    }

    this.readyState = ReadyState.CLOSING;

    if (code !== undefined) {
      this._ref.ws.end(code, data);

    } else {
      this._ref.ws.close();
    }
  }

  public close(code?: number, data?: string) {
    logger.warn('DEPRECATION WARNING: use client.leave() instead of client.close()');
    try {
      throw new Error();
    } catch (e) {
      logger.info(e.stack);
    }
    this.leave(code, data);
  }

  public toJSON() {
    return { sessionId: this.sessionId, readyState: this.readyState };
  }
}
