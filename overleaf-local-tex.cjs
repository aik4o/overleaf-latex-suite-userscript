/*
yauzl

The MIT License (MIT)

Copyright (c) 2014 Josh Wolfe

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.


pend

The MIT License (Expat)

Copyright (c) 2014 Andrew Kelley

Permission is hereby granted, free of charge, to any person
obtaining a copy of this software and associated documentation files
(the "Software"), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge,
publish, distribute, sublicense, and/or sell copies of the Software,
and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

*/
var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __commonJS = (cb, mod) => function __require() {
  try {
    return mod || (0, cb[__getOwnPropNames(cb)[0]])((mod = { exports: {} }).exports, mod), mod.exports;
  } catch (e) {
    throw mod = 0, e;
  }
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(
  // If the importer is in node compatibility mode or this is not an ESM
  // file that has been converted to a CommonJS file using a Babel-
  // compatible transform (i.e. "__esModule" has not been set), then set
  // "default" to the CommonJS "module.exports" for node compatibility.
  isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target,
  mod
));

// node_modules/pend/index.js
var require_pend = __commonJS({
  "node_modules/pend/index.js"(exports2, module2) {
    module2.exports = Pend;
    function Pend() {
      this.pending = 0;
      this.max = Infinity;
      this.listeners = [];
      this.waiting = [];
      this.error = null;
    }
    Pend.prototype.go = function(fn) {
      if (this.pending < this.max) {
        pendGo(this, fn);
      } else {
        this.waiting.push(fn);
      }
    };
    Pend.prototype.wait = function(cb) {
      if (this.pending === 0) {
        cb(this.error);
      } else {
        this.listeners.push(cb);
      }
    };
    Pend.prototype.hold = function() {
      return pendHold(this);
    };
    function pendHold(self) {
      self.pending += 1;
      var called = false;
      return onCb;
      function onCb(err) {
        if (called) throw new Error("callback called twice");
        called = true;
        self.error = self.error || err;
        self.pending -= 1;
        if (self.waiting.length > 0 && self.pending < self.max) {
          pendGo(self, self.waiting.shift());
        } else if (self.pending === 0) {
          var listeners = self.listeners;
          self.listeners = [];
          listeners.forEach(cbListener);
        }
      }
      function cbListener(listener) {
        listener(self.error);
      }
    }
    function pendGo(self, fn) {
      fn(pendHold(self));
    }
  }
});

// node_modules/yauzl/fd-slicer.js
var require_fd_slicer = __commonJS({
  "node_modules/yauzl/fd-slicer.js"(exports2) {
    var fs = require("fs");
    var util = require("util");
    var stream = require("stream");
    var Readable = stream.Readable;
    var PassThrough = stream.PassThrough;
    var Pend = require_pend();
    var EventEmitter = require("events").EventEmitter;
    exports2.BufferSlicer = BufferSlicer;
    exports2.FdSlicer = FdSlicer;
    util.inherits(FdSlicer, EventEmitter);
    function FdSlicer(fd) {
      EventEmitter.call(this);
      this.fd = fd;
      this.pend = new Pend();
      this.pend.max = 1;
      this.refCount = 0;
    }
    FdSlicer.prototype.read = function(buffer, offset, length, position, callback) {
      var self = this;
      self.pend.go(function(cb) {
        fs.read(self.fd, buffer, offset, length, position, function(err, bytesRead, buffer2) {
          cb();
          callback(err, bytesRead, buffer2);
        });
      });
    };
    FdSlicer.prototype.createReadStream = function(options2) {
      return new ReadStream(this, options2);
    };
    FdSlicer.prototype.ref = function() {
      this.refCount += 1;
    };
    FdSlicer.prototype.unref = function() {
      var self = this;
      self.refCount -= 1;
      if (self.refCount < 0) throw new Error("invalid unref");
      if (self.refCount > 0) return;
      fs.close(self.fd, onCloseDone);
      function onCloseDone(err) {
        if (err) {
          self.emit("error", err);
        } else {
          self.emit("close");
        }
      }
    };
    util.inherits(ReadStream, Readable);
    function ReadStream(context, options2) {
      options2 = options2 || {};
      Readable.call(this, options2);
      this.context = context;
      this.context.ref();
      this.start = options2.start || 0;
      this.endOffset = options2.end;
      this.pos = this.start;
    }
    ReadStream.prototype._read = function(n) {
      var self = this;
      var toRead = Math.min(self._readableState.highWaterMark, n);
      if (self.endOffset != null) {
        toRead = Math.min(toRead, self.endOffset - self.pos);
      }
      if (toRead <= 0) {
        self.push(null);
        this._cleanup();
        return;
      }
      self.context.pend.go(function(cb) {
        var buffer = Buffer.allocUnsafe(toRead);
        fs.read(self.context.fd, buffer, 0, toRead, self.pos, function(err, bytesRead) {
          if (err) {
            self.destroy(err);
          } else if (bytesRead === 0) {
            self.push(null);
            self._cleanup();
          } else {
            self.pos += bytesRead;
            self.push(buffer.slice(0, bytesRead));
          }
          cb();
        });
      });
    };
    ReadStream.prototype._destroy = function(err, cb) {
      this._cleanup();
      cb(err);
    };
    ReadStream.prototype._cleanup = function() {
      if (this.context != null) {
        this.context.unref();
        this.context = null;
      }
    };
    util.inherits(BufferSlicer, EventEmitter);
    function BufferSlicer(buffer) {
      EventEmitter.call(this);
      this.refCount = 0;
      this.buffer = buffer;
    }
    BufferSlicer.prototype.read = function(buffer, offset, length, position, callback) {
      if (!(0 <= offset && offset <= buffer.length)) throw new RangeError("offset outside buffer: 0 <= " + offset + " <= " + buffer.length);
      if (position < 0) throw new RangeError("position is negative: " + position);
      if (offset + length > buffer.length) {
        length = buffer.length - offset;
      }
      if (position + length > this.buffer.length) {
        length = this.buffer.length - position;
      }
      if (length <= 0) {
        setImmediate(function() {
          callback(null, 0);
        });
        return;
      }
      this.buffer.copy(buffer, offset, position, position + length);
      setImmediate(function() {
        callback(null, length);
      });
    };
    BufferSlicer.prototype.createReadStream = function(options2) {
      options2 = options2 || {};
      var readStream = new PassThrough(options2);
      readStream.start = options2.start || 0;
      readStream.endOffset = options2.end;
      readStream.pos = readStream.endOffset || this.buffer.length;
      var entireSlice = this.buffer.slice(readStream.start, readStream.pos);
      var maxChunkSize = 65536;
      var offset = 0;
      while (true) {
        var nextOffset = offset + maxChunkSize;
        if (nextOffset >= entireSlice.length) {
          if (offset < entireSlice.length) {
            readStream.write(entireSlice.slice(offset, entireSlice.length));
          }
          break;
        }
        readStream.write(entireSlice.slice(offset, nextOffset));
        offset = nextOffset;
      }
      readStream.end();
      return readStream;
    };
    BufferSlicer.prototype.ref = function() {
      this.refCount += 1;
    };
    BufferSlicer.prototype.unref = function() {
      this.refCount -= 1;
      if (this.refCount < 0) {
        throw new Error("invalid unref");
      }
    };
  }
});

// node_modules/yauzl/crc32.js
var require_crc32 = __commonJS({
  "node_modules/yauzl/crc32.js"(exports2, module2) {
    var CRC_TABLE = new Int32Array([
      0,
      1996959894,
      3993919788,
      2567524794,
      124634137,
      1886057615,
      3915621685,
      2657392035,
      249268274,
      2044508324,
      3772115230,
      2547177864,
      162941995,
      2125561021,
      3887607047,
      2428444049,
      498536548,
      1789927666,
      4089016648,
      2227061214,
      450548861,
      1843258603,
      4107580753,
      2211677639,
      325883990,
      1684777152,
      4251122042,
      2321926636,
      335633487,
      1661365465,
      4195302755,
      2366115317,
      997073096,
      1281953886,
      3579855332,
      2724688242,
      1006888145,
      1258607687,
      3524101629,
      2768942443,
      901097722,
      1119000684,
      3686517206,
      2898065728,
      853044451,
      1172266101,
      3705015759,
      2882616665,
      651767980,
      1373503546,
      3369554304,
      3218104598,
      565507253,
      1454621731,
      3485111705,
      3099436303,
      671266974,
      1594198024,
      3322730930,
      2970347812,
      795835527,
      1483230225,
      3244367275,
      3060149565,
      1994146192,
      31158534,
      2563907772,
      4023717930,
      1907459465,
      112637215,
      2680153253,
      3904427059,
      2013776290,
      251722036,
      2517215374,
      3775830040,
      2137656763,
      141376813,
      2439277719,
      3865271297,
      1802195444,
      476864866,
      2238001368,
      4066508878,
      1812370925,
      453092731,
      2181625025,
      4111451223,
      1706088902,
      314042704,
      2344532202,
      4240017532,
      1658658271,
      366619977,
      2362670323,
      4224994405,
      1303535960,
      984961486,
      2747007092,
      3569037538,
      1256170817,
      1037604311,
      2765210733,
      3554079995,
      1131014506,
      879679996,
      2909243462,
      3663771856,
      1141124467,
      855842277,
      2852801631,
      3708648649,
      1342533948,
      654459306,
      3188396048,
      3373015174,
      1466479909,
      544179635,
      3110523913,
      3462522015,
      1591671054,
      702138776,
      2966460450,
      3352799412,
      1504918807,
      783551873,
      3082640443,
      3233442989,
      3988292384,
      2596254646,
      62317068,
      1957810842,
      3939845945,
      2647816111,
      81470997,
      1943803523,
      3814918930,
      2489596804,
      225274430,
      2053790376,
      3826175755,
      2466906013,
      167816743,
      2097651377,
      4027552580,
      2265490386,
      503444072,
      1762050814,
      4150417245,
      2154129355,
      426522225,
      1852507879,
      4275313526,
      2312317920,
      282753626,
      1742555852,
      4189708143,
      2394877945,
      397917763,
      1622183637,
      3604390888,
      2714866558,
      953729732,
      1340076626,
      3518719985,
      2797360999,
      1068828381,
      1219638859,
      3624741850,
      2936675148,
      906185462,
      1090812512,
      3747672003,
      2825379669,
      829329135,
      1181335161,
      3412177804,
      3160834842,
      628085408,
      1382605366,
      3423369109,
      3138078467,
      570562233,
      1426400815,
      3317316542,
      2998733608,
      733239954,
      1555261956,
      3268935591,
      3050360625,
      752459403,
      1541320221,
      2607071920,
      3965973030,
      1969922972,
      40735498,
      2617837225,
      3943577151,
      1913087877,
      83908371,
      2512341634,
      3803740692,
      2075208622,
      213261112,
      2463272603,
      3855990285,
      2094854071,
      198958881,
      2262029012,
      4057260610,
      1759359992,
      534414190,
      2176718541,
      4139329115,
      1873836001,
      414664567,
      2282248934,
      4279200368,
      1711684554,
      285281116,
      2405801727,
      4167216745,
      1634467795,
      376229701,
      2685067896,
      3608007406,
      1308918612,
      956543938,
      2808555105,
      3495958263,
      1231636301,
      1047427035,
      2932959818,
      3654703836,
      1088359270,
      936918e3,
      2847714899,
      3736837829,
      1202900863,
      817233897,
      3183342108,
      3401237130,
      1404277552,
      615818150,
      3134207493,
      3453421203,
      1423857449,
      601450431,
      3009837614,
      3294710456,
      1567103746,
      711928724,
      3020668471,
      3272380065,
      1510334235,
      755167117
    ]);
    function crc32(buf) {
      let crc = -1;
      for (let x of buf) {
        crc = CRC_TABLE[(crc ^ x) & 255] ^ crc >>> 8;
      }
      return (crc ^ -1) >>> 0;
    }
    module2.exports = crc32;
  }
});

// node_modules/yauzl/index.js
var require_yauzl = __commonJS({
  "node_modules/yauzl/index.js"(exports2) {
    var fs = require("fs");
    var zlib = require("zlib");
    var fd_slicer = require_fd_slicer();
    var util = require("util");
    var EventEmitter = require("events").EventEmitter;
    var Transform = require("stream").Transform;
    var PassThrough = require("stream").PassThrough;
    var Writable = require("stream").Writable;
    var crc32 = typeof zlib.crc32 === "function" ? zlib.crc32 : require_crc32();
    exports2.open = open2;
    exports2.fromFd = fromFd;
    exports2.fromBuffer = fromBuffer;
    exports2.fromRandomAccessReader = fromRandomAccessReader;
    exports2.openPromise = openPromise;
    exports2.fromFdPromise = fromFdPromise;
    exports2.fromBufferPromise = fromBufferPromise;
    exports2.fromRandomAccessReaderPromise = fromRandomAccessReaderPromise;
    exports2.dosDateTimeToDate = dosDateTimeToDate;
    exports2.getFileNameLowLevel = getFileNameLowLevel;
    exports2.validateFileName = validateFileName;
    exports2.parseExtraFields = parseExtraFields;
    exports2.ZipFile = ZipFile;
    exports2.Entry = Entry;
    exports2.LocalFileHeader = LocalFileHeader;
    exports2.RandomAccessReader = RandomAccessReader;
    function openPromise(path2, options2) {
      return new Promise((resolve, reject) => {
        open2(path2, { ...options2, lazyEntries: true }, function(err, zipfile) {
          if (err) return reject(err);
          resolve(zipfile);
        });
      });
    }
    function fromFdPromise(fd, options2) {
      return new Promise((resolve, reject) => {
        fromFd(fd, { ...options2, lazyEntries: true }, function(err, zipfile) {
          if (err) return reject(err);
          resolve(zipfile);
        });
      });
    }
    function fromBufferPromise(buffer, options2) {
      return new Promise((resolve, reject) => {
        fromBuffer(buffer, { ...options2, lazyEntries: true }, function(err, zipfile) {
          if (err) return reject(err);
          resolve(zipfile);
        });
      });
    }
    function fromRandomAccessReaderPromise(reader, totalSize, options2) {
      return new Promise((resolve, reject) => {
        fromRandomAccessReader(reader, totalSize, { ...options2, lazyEntries: true }, function(err, zipfile) {
          if (err) return reject(err);
          resolve(zipfile);
        });
      });
    }
    function open2(path2, options2, callback) {
      if (typeof options2 === "function") {
        callback = options2;
        options2 = null;
      }
      if (options2 == null) options2 = {};
      if (options2.autoClose == null) options2.autoClose = true;
      if (options2.lazyEntries == null) options2.lazyEntries = false;
      if (options2.decodeStrings == null) options2.decodeStrings = true;
      if (options2.validateEntrySizes == null) options2.validateEntrySizes = true;
      if (options2.strictFileNames == null) options2.strictFileNames = false;
      if (callback == null) callback = defaultCallback;
      fs.open(path2, "r", function(err, fd) {
        if (err) return callback(err);
        fromFd(fd, options2, function(err2, zipfile) {
          if (err2) fs.close(fd, defaultCallback);
          callback(err2, zipfile);
        });
      });
    }
    function fromFd(fd, options2, callback) {
      if (typeof options2 === "function") {
        callback = options2;
        options2 = null;
      }
      if (options2 == null) options2 = {};
      if (options2.autoClose == null) options2.autoClose = false;
      if (options2.lazyEntries == null) options2.lazyEntries = false;
      if (options2.decodeStrings == null) options2.decodeStrings = true;
      if (options2.validateEntrySizes == null) options2.validateEntrySizes = true;
      if (options2.strictFileNames == null) options2.strictFileNames = false;
      if (callback == null) callback = defaultCallback;
      fs.fstat(fd, function(err, stats) {
        if (err) return callback(err);
        var reader = new fd_slicer.FdSlicer(fd);
        fromRandomAccessReader(reader, stats.size, options2, callback);
      });
    }
    function fromBuffer(buffer, options2, callback) {
      if (typeof options2 === "function") {
        callback = options2;
        options2 = null;
      }
      if (options2 == null) options2 = {};
      options2.autoClose = false;
      if (options2.lazyEntries == null) options2.lazyEntries = false;
      if (options2.decodeStrings == null) options2.decodeStrings = true;
      if (options2.validateEntrySizes == null) options2.validateEntrySizes = true;
      if (options2.strictFileNames == null) options2.strictFileNames = false;
      var reader = new fd_slicer.BufferSlicer(buffer);
      fromRandomAccessReader(reader, buffer.length, options2, callback);
    }
    function fromRandomAccessReader(reader, totalSize, options2, callback) {
      if (typeof options2 === "function") {
        callback = options2;
        options2 = null;
      }
      if (options2 == null) options2 = {};
      if (options2.autoClose == null) options2.autoClose = true;
      if (options2.lazyEntries == null) options2.lazyEntries = false;
      if (options2.decodeStrings == null) options2.decodeStrings = true;
      var decodeStrings = !!options2.decodeStrings;
      if (options2.validateEntrySizes == null) options2.validateEntrySizes = true;
      if (options2.strictFileNames == null) options2.strictFileNames = false;
      if (callback == null) callback = defaultCallback;
      if (typeof totalSize !== "number") throw new Error("expected totalSize parameter to be a number");
      if (totalSize > Number.MAX_SAFE_INTEGER) {
        throw new Error("zip file too large. only file sizes up to 2^52 are supported due to JavaScript's Number type being an IEEE 754 double.");
      }
      reader.ref();
      var eocdrWithoutCommentSize = 22;
      var zip64EocdlSize = 20;
      var maxCommentSize = 65535;
      var bufferSize = Math.min(zip64EocdlSize + eocdrWithoutCommentSize + maxCommentSize, totalSize);
      var buffer = newBuffer(bufferSize);
      var bufferReadStart = totalSize - buffer.length;
      readAndAssertNoEof(reader, buffer, 0, bufferSize, bufferReadStart, function(err) {
        if (err) return callback(err);
        for (var i = bufferSize - eocdrWithoutCommentSize; i >= 0; i -= 1) {
          if (buffer.readUInt32LE(i) !== 101010256) continue;
          var eocdrBuffer = buffer.subarray(i);
          var diskNumber = eocdrBuffer.readUInt16LE(4);
          var entryCount = eocdrBuffer.readUInt16LE(10);
          var centralDirectoryOffset = eocdrBuffer.readUInt32LE(16);
          var commentLength = eocdrBuffer.readUInt16LE(20);
          var expectedCommentLength = eocdrBuffer.length - eocdrWithoutCommentSize;
          if (commentLength !== expectedCommentLength) {
            return callback(new Error("Invalid comment length. Expected: " + expectedCommentLength + ". Found: " + commentLength + ". Are there extra bytes at the end of the file? Or is the end of central dir signature `PK\u263A\u263B` in the comment?"));
          }
          var comment = decodeStrings ? decodeBuffer(eocdrBuffer.subarray(22), false) : eocdrBuffer.subarray(22);
          if (i - zip64EocdlSize >= 0 && buffer.readUInt32LE(i - zip64EocdlSize) === 117853008) {
            var zip64EocdlBuffer = buffer.subarray(i - zip64EocdlSize, i - zip64EocdlSize + zip64EocdlSize);
            var zip64EocdrOffset = readUInt64LE(zip64EocdlBuffer, 8);
            var zip64EocdrBuffer = newBuffer(56);
            return readAndAssertNoEof(reader, zip64EocdrBuffer, 0, zip64EocdrBuffer.length, zip64EocdrOffset, function(err2) {
              if (err2) return callback(err2);
              if (zip64EocdrBuffer.readUInt32LE(0) !== 101075792) {
                return callback(new Error("invalid zip64 end of central directory record signature"));
              }
              diskNumber = zip64EocdrBuffer.readUInt32LE(16);
              if (diskNumber !== 0) {
                return callback(new Error("multi-disk zip files are not supported: found disk number: " + diskNumber));
              }
              entryCount = readUInt64LE(zip64EocdrBuffer, 32);
              centralDirectoryOffset = readUInt64LE(zip64EocdrBuffer, 48);
              return callback(null, new ZipFile(reader, centralDirectoryOffset, totalSize, entryCount, comment, options2.autoClose, options2.lazyEntries, decodeStrings, options2.validateEntrySizes, options2.strictFileNames));
            });
          }
          if (diskNumber !== 0) {
            return callback(new Error("multi-disk zip files are not supported: found disk number: " + diskNumber));
          }
          return callback(null, new ZipFile(reader, centralDirectoryOffset, totalSize, entryCount, comment, options2.autoClose, options2.lazyEntries, decodeStrings, options2.validateEntrySizes, options2.strictFileNames));
        }
        callback(new Error("End of central directory record signature not found. Either not a zip file, or file is truncated."));
      });
    }
    util.inherits(ZipFile, EventEmitter);
    function ZipFile(reader, centralDirectoryOffset, fileSize, entryCount, comment, autoClose, lazyEntries, decodeStrings, validateEntrySizes, strictFileNames) {
      var self = this;
      EventEmitter.call(self);
      self.reader = reader;
      self.reader.on("error", function(err) {
        emitError(self, err);
      });
      self.reader.once("close", function() {
        self.emit("close");
      });
      self.readEntryCursor = centralDirectoryOffset;
      self.fileSize = fileSize;
      self.entryCount = entryCount;
      self.comment = comment;
      self.entriesRead = 0;
      self.autoClose = !!autoClose;
      self.lazyEntries = !!lazyEntries;
      self.decodeStrings = !!decodeStrings;
      self.validateEntrySizes = !!validateEntrySizes;
      self.strictFileNames = !!strictFileNames;
      self.isOpen = true;
      self.emittedError = false;
      self.hasEachEntryBeenCalled = false;
      if (!self.lazyEntries) self._readEntry();
    }
    ZipFile.prototype.close = function() {
      if (!this.isOpen) return;
      this.isOpen = false;
      this.reader.unref();
    };
    function emitErrorAndAutoClose(self, err) {
      if (self.autoClose) self.close();
      emitError(self, err);
    }
    function emitError(self, err) {
      if (self.emittedError) return;
      self.emittedError = true;
      self.emit("error", err);
    }
    ZipFile.prototype.readEntry = function() {
      if (!this.lazyEntries) throw new Error("readEntry() called without lazyEntries:true");
      this._readEntry();
    };
    ZipFile.prototype._readEntry = function() {
      var self = this;
      if (self.entryCount === self.entriesRead) {
        setImmediate(function() {
          if (self.autoClose) self.close();
          if (self.emittedError) return;
          self.emit("end");
        });
        return;
      }
      if (self.emittedError) return;
      var buffer = newBuffer(46);
      readAndAssertNoEof(self.reader, buffer, 0, buffer.length, self.readEntryCursor, function(err) {
        if (err) return emitErrorAndAutoClose(self, err);
        if (self.emittedError) return;
        var entry = new Entry();
        var signature = buffer.readUInt32LE(0);
        if (signature !== 33639248) return emitErrorAndAutoClose(self, new Error("invalid central directory file header signature: 0x" + signature.toString(16)));
        entry.versionMadeBy = buffer.readUInt16LE(4);
        entry.versionNeededToExtract = buffer.readUInt16LE(6);
        entry.generalPurposeBitFlag = buffer.readUInt16LE(8);
        entry.compressionMethod = buffer.readUInt16LE(10);
        entry.lastModFileTime = buffer.readUInt16LE(12);
        entry.lastModFileDate = buffer.readUInt16LE(14);
        entry.crc32 = buffer.readUInt32LE(16);
        entry.compressedSize = buffer.readUInt32LE(20);
        entry.uncompressedSize = buffer.readUInt32LE(24);
        entry.fileNameLength = buffer.readUInt16LE(28);
        entry.extraFieldLength = buffer.readUInt16LE(30);
        entry.fileCommentLength = buffer.readUInt16LE(32);
        entry.internalFileAttributes = buffer.readUInt16LE(36);
        entry.externalFileAttributes = buffer.readUInt32LE(38);
        entry.relativeOffsetOfLocalHeader = buffer.readUInt32LE(42);
        if (entry.generalPurposeBitFlag & 64) return emitErrorAndAutoClose(self, new Error("strong encryption is not supported"));
        self.readEntryCursor += 46;
        buffer = newBuffer(entry.fileNameLength + entry.extraFieldLength + entry.fileCommentLength);
        readAndAssertNoEof(self.reader, buffer, 0, buffer.length, self.readEntryCursor, function(err2) {
          if (err2) return emitErrorAndAutoClose(self, err2);
          if (self.emittedError) return;
          entry.fileNameRaw = buffer.subarray(0, entry.fileNameLength);
          var fileCommentStart = entry.fileNameLength + entry.extraFieldLength;
          entry.extraFieldRaw = buffer.subarray(entry.fileNameLength, fileCommentStart);
          entry.fileCommentRaw = buffer.subarray(fileCommentStart, fileCommentStart + entry.fileCommentLength);
          try {
            entry.extraFields = parseExtraFields(entry.extraFieldRaw);
          } catch (err3) {
            return emitErrorAndAutoClose(self, err3);
          }
          if (self.decodeStrings) {
            var isUtf8 = (entry.generalPurposeBitFlag & 2048) !== 0;
            entry.fileComment = decodeBuffer(entry.fileCommentRaw, isUtf8);
            entry.fileName = getFileNameLowLevel(entry.generalPurposeBitFlag, entry.fileNameRaw, entry.extraFields, self.strictFileNames);
            var errorMessage = validateFileName(entry.fileName);
            if (errorMessage != null) return emitErrorAndAutoClose(self, new Error(errorMessage));
          } else {
            entry.fileComment = entry.fileCommentRaw;
            entry.fileName = entry.fileNameRaw;
          }
          entry.comment = entry.fileComment;
          self.readEntryCursor += buffer.length;
          self.entriesRead += 1;
          for (var i = 0; i < entry.extraFields.length; i++) {
            var extraField = entry.extraFields[i];
            if (extraField.id !== 1) continue;
            var zip64EiefBuffer = extraField.data;
            var index = 0;
            if (entry.uncompressedSize === 4294967295) {
              if (index + 8 > zip64EiefBuffer.length) {
                return emitErrorAndAutoClose(self, new Error("zip64 extended information extra field does not include uncompressed size"));
              }
              entry.uncompressedSize = readUInt64LE(zip64EiefBuffer, index);
              index += 8;
            }
            if (entry.compressedSize === 4294967295) {
              if (index + 8 > zip64EiefBuffer.length) {
                return emitErrorAndAutoClose(self, new Error("zip64 extended information extra field does not include compressed size"));
              }
              entry.compressedSize = readUInt64LE(zip64EiefBuffer, index);
              index += 8;
            }
            if (entry.relativeOffsetOfLocalHeader === 4294967295) {
              if (index + 8 > zip64EiefBuffer.length) {
                return emitErrorAndAutoClose(self, new Error("zip64 extended information extra field does not include relative header offset"));
              }
              entry.relativeOffsetOfLocalHeader = readUInt64LE(zip64EiefBuffer, index);
              index += 8;
            }
            break;
          }
          if (self.validateEntrySizes && entry.compressionMethod === 0) {
            var expectedCompressedSize = entry.uncompressedSize;
            if (entry.isEncrypted()) {
              expectedCompressedSize += 12;
            }
            if (entry.compressedSize !== expectedCompressedSize) {
              var msg = "compressed/uncompressed size mismatch for stored file: " + entry.compressedSize + " != " + entry.uncompressedSize;
              return emitErrorAndAutoClose(self, new Error(msg));
            }
          }
          self.emit("entry", entry);
          if (!self.lazyEntries) self._readEntry();
        });
      });
    };
    ZipFile.prototype.eachEntry = function() {
      const self = this;
      if (!self.lazyEntries) throw new Error("eachEntry() requires lazyEntries: true");
      if (self.hasEachEntryBeenCalled) throw new Error("eachEntry() must only be called once per ZipFile");
      self.hasEachEntryBeenCalled = true;
      let pendingResolveReject = null;
      self.on("entry", onEntry);
      self.on("end", onEnd);
      self.on("error", onError);
      function cleanup() {
        self.removeListener("entry", onEntry);
        self.removeListener("end", onEnd);
        self.removeListener("error", onError);
        if (self.autoClose) self.close();
      }
      function onEntry(entry) {
        let { resolve } = pendingResolveReject;
        pendingResolveReject = null;
        resolve({ value: entry });
      }
      function onEnd() {
        let { resolve } = pendingResolveReject;
        pendingResolveReject = null;
        cleanup();
        resolve({ done: true });
      }
      function onError(err) {
        let { reject } = pendingResolveReject;
        pendingResolveReject = null;
        cleanup();
        reject(err);
      }
      return {
        [Symbol.asyncIterator]() {
          return this;
        },
        next() {
          const promise = new Promise((resolve, reject) => {
            if (pendingResolveReject != null) throw new Error("next() called before previous Promise was resolved.");
            pendingResolveReject = { resolve, reject };
          });
          self.readEntry();
          return promise;
        },
        return(value) {
          cleanup();
          return Promise.resolve({ done: true, value });
        },
        throw(value) {
          cleanup();
          return Promise.reject(value);
        }
      };
    };
    ZipFile.prototype.openReadStream = function(entry, options2, callback) {
      var self = this;
      var relativeStart = 0;
      var relativeEnd = entry.compressedSize;
      if (callback == null) {
        callback = options2;
        options2 = null;
      }
      if (options2 == null) {
        options2 = {};
      } else {
        if (options2.decodeFileData === false) {
          if (options2.decrypt != null) {
            throw new Error("cannot use options.decrypt when options.decodeFileData === false");
          }
          if (options2.decompress != null) {
            throw new Error("cannot use options.decompress when options.decodeFileData === false");
          }
        } else {
          if (options2.decrypt != null) {
            if (!entry.isEncrypted()) {
              throw new Error("options.decrypt can only be specified for encrypted entries. See also option decodeFileData.");
            }
            if (options2.decrypt !== false) throw new Error("invalid options.decrypt value: " + options2.decrypt);
            if (entry.isCompressed()) {
              if (options2.decompress !== false) throw new Error("entry is encrypted and compressed, and options.decompress !== false. See also option decodeFileData.");
            }
          }
          if (options2.decompress != null) {
            if (!entry.isCompressed()) {
              throw new Error("options.decompress can only be specified for compressed entries. See also option decodeFileData.");
            }
            if (!(options2.decompress === false || options2.decompress === true)) {
              throw new Error("invalid options.decompress value: " + options2.decompress);
            }
            decompress = options2.decompress;
          }
        }
        if (options2.start != null) {
          relativeStart = options2.start;
          if (relativeStart < 0) throw new Error("options.start < 0");
          if (relativeStart > entry.compressedSize) throw new Error("options.start > entry.compressedSize");
        }
        if (options2.end != null) {
          relativeEnd = options2.end;
          if (relativeEnd < 0) throw new Error("options.end < 0");
          if (relativeEnd > entry.compressedSize) throw new Error("options.end > entry.compressedSize");
          if (relativeEnd < relativeStart) throw new Error("options.end < options.start");
        }
      }
      var rawMode = options2.decodeFileData === false || // Explicitly requested raw.
      (entry.compressionMethod === 0 || // Naturally without compression.
      entry.compressionMethod === 8 && options2.decompress === false) && (!entry.isEncrypted() || // Naturally without encryption.
      options2.decrypt === false);
      if (options2.start != null || options2.end != null) {
        if (!rawMode) throw new Error("start/end range require options.decodeFileData === false for non-trivial encoded entries.");
      }
      if (!self.isOpen) return callback(new Error("closed"));
      if (entry.isEncrypted() && !rawMode) {
        if (options2.decrypt !== false) return callback(new Error("entry is encrypted, and options.decodeFileData !== false"));
      }
      var decompress;
      if (rawMode) {
        decompress = false;
      } else if (entry.compressionMethod === 8) {
        decompress = options2.decodeFileData !== true;
      } else {
        return callback(new Error("unsupported compression method: " + entry.compressionMethod));
      }
      self.readLocalFileHeader(entry, { minimal: true }, function(err, localFileHeader) {
        if (err) return callback(err);
        self.openReadStreamLowLevel(
          localFileHeader.fileDataStart,
          entry.compressedSize,
          relativeStart,
          relativeEnd,
          decompress,
          entry.uncompressedSize,
          callback
        );
      });
    };
    ZipFile.prototype.openReadStreamLowLevel = function(fileDataStart, compressedSize, relativeStart, relativeEnd, decompress, uncompressedSize, callback) {
      var self = this;
      var fileDataEnd = fileDataStart + compressedSize;
      var readStream = self.reader.createReadStream({
        start: fileDataStart + relativeStart,
        end: fileDataStart + relativeEnd
      });
      var endpointStream = readStream;
      if (decompress) {
        var destroyed = false;
        var inflateFilter = zlib.createInflateRaw();
        readStream.on("error", function(err) {
          setImmediate(function() {
            if (!destroyed) inflateFilter.emit("error", err);
          });
        });
        readStream.pipe(inflateFilter);
        if (self.validateEntrySizes) {
          endpointStream = new AssertByteCountStream(uncompressedSize);
          inflateFilter.on("error", function(err) {
            setImmediate(function() {
              if (!destroyed) endpointStream.emit("error", err);
            });
          });
          inflateFilter.pipe(endpointStream);
        } else {
          endpointStream = inflateFilter;
        }
        installDestroyFn(endpointStream, function() {
          destroyed = true;
          if (inflateFilter !== endpointStream) inflateFilter.unpipe(endpointStream);
          readStream.unpipe(inflateFilter);
          readStream.destroy();
        });
      }
      callback(null, endpointStream);
    };
    ZipFile.prototype.readLocalFileHeader = function(entry, options2, callback) {
      var self = this;
      if (callback == null) {
        callback = options2;
        options2 = null;
      }
      if (options2 == null) options2 = {};
      self.reader.ref();
      var buffer = newBuffer(30);
      readAndAssertNoEof(self.reader, buffer, 0, buffer.length, entry.relativeOffsetOfLocalHeader, function(err) {
        try {
          if (err) return callback(err);
          var signature = buffer.readUInt32LE(0);
          if (signature !== 67324752) {
            return callback(new Error("invalid local file header signature: 0x" + signature.toString(16)));
          }
          var fileNameLength = buffer.readUInt16LE(26);
          var extraFieldLength = buffer.readUInt16LE(28);
          var fileDataStart = entry.relativeOffsetOfLocalHeader + 30 + fileNameLength + extraFieldLength;
          if (fileDataStart + entry.compressedSize > self.fileSize) {
            return callback(new Error("file data overflows file bounds: " + fileDataStart + " + " + entry.compressedSize + " > " + self.fileSize));
          }
          if (options2.minimal) {
            return callback(null, { fileDataStart });
          }
          var localFileHeader = new LocalFileHeader();
          localFileHeader.fileDataStart = fileDataStart;
          localFileHeader.versionNeededToExtract = buffer.readUInt16LE(4);
          localFileHeader.generalPurposeBitFlag = buffer.readUInt16LE(6);
          localFileHeader.compressionMethod = buffer.readUInt16LE(8);
          localFileHeader.lastModFileTime = buffer.readUInt16LE(10);
          localFileHeader.lastModFileDate = buffer.readUInt16LE(12);
          localFileHeader.crc32 = buffer.readUInt32LE(14);
          localFileHeader.compressedSize = buffer.readUInt32LE(18);
          localFileHeader.uncompressedSize = buffer.readUInt32LE(22);
          localFileHeader.fileNameLength = fileNameLength;
          localFileHeader.extraFieldLength = extraFieldLength;
          buffer = newBuffer(fileNameLength + extraFieldLength);
          self.reader.ref();
          readAndAssertNoEof(self.reader, buffer, 0, buffer.length, entry.relativeOffsetOfLocalHeader + 30, function(err2) {
            try {
              if (err2) return callback(err2);
              localFileHeader.fileName = buffer.subarray(0, fileNameLength);
              localFileHeader.extraField = buffer.subarray(fileNameLength);
              return callback(null, localFileHeader);
            } finally {
              self.reader.unref();
            }
          });
        } finally {
          self.reader.unref();
        }
      });
    };
    ZipFile.prototype.openReadStreamPromise = function(entry, options2) {
      return new Promise((resolve, reject) => {
        this.openReadStream(entry, options2, function(err, readStream) {
          if (err) return reject(err);
          resolve(readStream);
        });
      });
    };
    ZipFile.prototype.openReadStreamLowLevelPromise = function(fileDataStart, compressedSize, relativeStart, relativeEnd, decompress, uncompressedSize) {
      return new Promise((resolve, reject) => {
        this.openReadStream(fileDataStart, compressedSize, relativeStart, relativeEnd, decompress, uncompressedSize, function(err, readStream) {
          if (err) return reject(err);
          resolve(readStream);
        });
      });
    };
    ZipFile.prototype.readLocalFileHeaderPromise = function(entry, options2) {
      return new Promise((resolve, reject) => {
        this.readLocalFileHeader(entry, options2, function(err, localFileHeader) {
          if (err) return reject(err);
          resolve(localFileHeader);
        });
      });
    };
    function Entry() {
    }
    Entry.prototype.getLastModDate = function(options2) {
      if (options2 == null) options2 = {};
      if (!options2.forceDosFormat) {
        for (var i = 0; i < this.extraFields.length; i++) {
          var extraField = this.extraFields[i];
          if (extraField.id === 21589) {
            var data = extraField.data;
            if (data.length < 5) continue;
            var flags = data[0];
            var HAS_MTIME = 1;
            if (!(flags & HAS_MTIME)) continue;
            var posixTimestamp = data.readInt32LE(1);
            return new Date(posixTimestamp * 1e3);
          } else if (extraField.id === 10) {
            var data = extraField.data;
            if (data.length !== 32) continue;
            if (data.readUInt16LE(4) !== 1) continue;
            if (data.readUInt16LE(6) !== 24) continue;
            var hundredNanoSecondsSince1601 = data.readUInt32LE(8) + 4294967296 * data.readInt32LE(12);
            var millisecondsSince1970 = hundredNanoSecondsSince1601 / 1e4 - 116444736e5;
            return new Date(millisecondsSince1970);
          }
        }
      }
      return dosDateTimeToDate(this.lastModFileDate, this.lastModFileTime, options2.timezone);
    };
    Entry.prototype.canDecodeFileData = function() {
      return !this.isEncrypted() && (this.compressionMethod === 0 || this.compressionMethod === 8);
    };
    Entry.prototype.isEncrypted = function() {
      return (this.generalPurposeBitFlag & 1) !== 0;
    };
    Entry.prototype.isCompressed = function() {
      return this.compressionMethod === 8;
    };
    function LocalFileHeader() {
    }
    function dosDateTimeToDate(date, time, timezone) {
      var day = date & 31;
      var month = (date >> 5 & 15) - 1;
      var year = (date >> 9 & 127) + 1980;
      var millisecond = 0;
      var second = (time & 31) * 2;
      var minute = time >> 5 & 63;
      var hour = time >> 11 & 31;
      if (timezone == null || timezone === "local") {
        return new Date(year, month, day, hour, minute, second, millisecond);
      } else if (timezone === "UTC") {
        return new Date(Date.UTC(year, month, day, hour, minute, second, millisecond));
      } else {
        throw new Error("unrecognized options.timezone: " + options.timezone);
      }
    }
    function getFileNameLowLevel(generalPurposeBitFlag, fileNameBuffer, extraFields, strictFileNames) {
      var fileName = null;
      for (var i = 0; i < extraFields.length; i++) {
        var extraField = extraFields[i];
        if (extraField.id === 28789) {
          if (extraField.data.length < 6) {
            continue;
          }
          if (extraField.data.readUInt8(0) !== 1) {
            continue;
          }
          var oldNameCrc32 = extraField.data.readUInt32LE(1);
          if (crc32(fileNameBuffer) !== oldNameCrc32) {
            continue;
          }
          fileName = decodeBuffer(extraField.data.subarray(5), true);
          break;
        }
      }
      if (fileName == null) {
        var isUtf8 = (generalPurposeBitFlag & 2048) !== 0;
        fileName = decodeBuffer(fileNameBuffer, isUtf8);
      }
      if (!strictFileNames) {
        fileName = fileName.replace(/\\/g, "/");
      }
      return fileName;
    }
    function validateFileName(fileName) {
      if (fileName.indexOf("\\") !== -1) {
        return "invalid characters in fileName: " + fileName;
      }
      if (/^[a-zA-Z]:/.test(fileName) || /^\//.test(fileName)) {
        return "absolute path: " + fileName;
      }
      if (fileName.split("/").indexOf("..") !== -1) {
        return "invalid relative path: " + fileName;
      }
      return null;
    }
    function parseExtraFields(extraFieldBuffer) {
      var extraFields = [];
      var i = 0;
      while (i < extraFieldBuffer.length - 3) {
        var headerId = extraFieldBuffer.readUInt16LE(i + 0);
        var dataSize = extraFieldBuffer.readUInt16LE(i + 2);
        var dataStart = i + 4;
        var dataEnd = dataStart + dataSize;
        if (dataEnd > extraFieldBuffer.length) throw new Error("extra field length exceeds extra field buffer size");
        var dataBuffer = extraFieldBuffer.subarray(dataStart, dataEnd);
        extraFields.push({
          id: headerId,
          data: dataBuffer
        });
        i = dataEnd;
      }
      return extraFields;
    }
    function readAndAssertNoEof(reader, buffer, offset, length, position, callback) {
      if (length === 0) {
        return setImmediate(function() {
          callback(null, newBuffer(0));
        });
      }
      reader.read(buffer, offset, length, position, function(err, bytesRead) {
        if (err) return callback(err);
        if (bytesRead < length) {
          return callback(new Error("unexpected EOF"));
        }
        callback();
      });
    }
    util.inherits(AssertByteCountStream, Transform);
    function AssertByteCountStream(byteCount) {
      Transform.call(this);
      this.actualByteCount = 0;
      this.expectedByteCount = byteCount;
    }
    AssertByteCountStream.prototype._transform = function(chunk, encoding, cb) {
      this.actualByteCount += chunk.length;
      if (this.actualByteCount > this.expectedByteCount) {
        var msg = "too many bytes in the stream. expected " + this.expectedByteCount + ". got at least " + this.actualByteCount;
        return cb(new Error(msg));
      }
      cb(null, chunk);
    };
    AssertByteCountStream.prototype._flush = function(cb) {
      if (this.actualByteCount < this.expectedByteCount) {
        var msg = "not enough bytes in the stream. expected " + this.expectedByteCount + ". got only " + this.actualByteCount;
        return cb(new Error(msg));
      }
      cb();
    };
    util.inherits(RandomAccessReader, EventEmitter);
    function RandomAccessReader() {
      EventEmitter.call(this);
      this.refCount = 0;
    }
    RandomAccessReader.prototype.ref = function() {
      this.refCount += 1;
    };
    RandomAccessReader.prototype.unref = function() {
      var self = this;
      self.refCount -= 1;
      if (self.refCount > 0) return;
      if (self.refCount < 0) throw new Error("invalid unref");
      self.close(onCloseDone);
      function onCloseDone(err) {
        if (err) return self.emit("error", err);
        self.emit("close");
      }
    };
    RandomAccessReader.prototype.createReadStream = function(options2) {
      if (options2 == null) options2 = {};
      var start = options2.start;
      var end = options2.end;
      if (start === end) {
        var emptyStream = new PassThrough();
        setImmediate(function() {
          emptyStream.end();
        });
        return emptyStream;
      }
      var stream = this._readStreamForRange(start, end);
      var destroyed = false;
      var refUnrefFilter = new RefUnrefFilter(this);
      stream.on("error", function(err) {
        setImmediate(function() {
          if (!destroyed) refUnrefFilter.emit("error", err);
        });
      });
      installDestroyFn(refUnrefFilter, function() {
        stream.unpipe(refUnrefFilter);
        refUnrefFilter.unref();
        stream.destroy();
      });
      var byteCounter = new AssertByteCountStream(end - start);
      refUnrefFilter.on("error", function(err) {
        setImmediate(function() {
          if (!destroyed) byteCounter.emit("error", err);
        });
      });
      installDestroyFn(byteCounter, function() {
        destroyed = true;
        refUnrefFilter.unpipe(byteCounter);
        refUnrefFilter.destroy();
      });
      return stream.pipe(refUnrefFilter).pipe(byteCounter);
    };
    RandomAccessReader.prototype._readStreamForRange = function(start, end) {
      throw new Error("not implemented");
    };
    RandomAccessReader.prototype.read = function(buffer, offset, length, position, callback) {
      var readStream = this.createReadStream({ start: position, end: position + length });
      var writeStream = new Writable();
      var written = 0;
      writeStream._write = function(chunk, encoding, cb) {
        chunk.copy(buffer, offset + written, 0, chunk.length);
        written += chunk.length;
        cb();
      };
      writeStream.on("finish", callback);
      readStream.on("error", function(error) {
        callback(error);
      });
      readStream.pipe(writeStream);
    };
    RandomAccessReader.prototype.close = function(callback) {
      setImmediate(callback);
    };
    util.inherits(RefUnrefFilter, PassThrough);
    function RefUnrefFilter(context) {
      PassThrough.call(this);
      this.context = context;
      this.context.ref();
      this.unreffedYet = false;
    }
    RefUnrefFilter.prototype._flush = function(cb) {
      this.unref();
      cb();
    };
    RefUnrefFilter.prototype.unref = function(cb) {
      if (this.unreffedYet) return;
      this.unreffedYet = true;
      this.context.unref();
    };
    var cp437 = "\0\u263A\u263B\u2665\u2666\u2663\u2660\u2022\u25D8\u25CB\u25D9\u2642\u2640\u266A\u266B\u263C\u25BA\u25C4\u2195\u203C\xB6\xA7\u25AC\u21A8\u2191\u2193\u2192\u2190\u221F\u2194\u25B2\u25BC !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~\u2302\xC7\xFC\xE9\xE2\xE4\xE0\xE5\xE7\xEA\xEB\xE8\xEF\xEE\xEC\xC4\xC5\xC9\xE6\xC6\xF4\xF6\xF2\xFB\xF9\xFF\xD6\xDC\xA2\xA3\xA5\u20A7\u0192\xE1\xED\xF3\xFA\xF1\xD1\xAA\xBA\xBF\u2310\xAC\xBD\xBC\xA1\xAB\xBB\u2591\u2592\u2593\u2502\u2524\u2561\u2562\u2556\u2555\u2563\u2551\u2557\u255D\u255C\u255B\u2510\u2514\u2534\u252C\u251C\u2500\u253C\u255E\u255F\u255A\u2554\u2569\u2566\u2560\u2550\u256C\u2567\u2568\u2564\u2565\u2559\u2558\u2552\u2553\u256B\u256A\u2518\u250C\u2588\u2584\u258C\u2590\u2580\u03B1\xDF\u0393\u03C0\u03A3\u03C3\xB5\u03C4\u03A6\u0398\u03A9\u03B4\u221E\u03C6\u03B5\u2229\u2261\xB1\u2265\u2264\u2320\u2321\xF7\u2248\xB0\u2219\xB7\u221A\u207F\xB2\u25A0\xA0";
    function decodeBuffer(buffer, isUtf8) {
      if (isUtf8) {
        return buffer.toString("utf8");
      } else {
        var result = "";
        for (var i = 0; i < buffer.length; i++) {
          result += cp437[buffer[i]];
        }
        return result;
      }
    }
    function readUInt64LE(buffer, offset) {
      var lower32 = buffer.readUInt32LE(offset);
      var upper32 = buffer.readUInt32LE(offset + 4);
      return upper32 * 4294967296 + lower32;
    }
    var newBuffer;
    if (typeof Buffer.allocUnsafe === "function") {
      newBuffer = function(len) {
        return Buffer.allocUnsafe(len);
      };
    } else {
      newBuffer = function(len) {
        return new Buffer(len);
      };
    }
    function installDestroyFn(stream, fn) {
      if (typeof stream.destroy === "function") {
        stream._destroy = function(err, cb) {
          fn();
          if (cb != null) cb(err);
        };
      } else {
        stream.destroy = fn;
      }
    }
    function defaultCallback(err) {
      if (err) throw err;
    }
  }
});

// local-tex/server.mjs
var import_node_crypto = require("node:crypto");
var import_node_http = __toESM(require("node:http"), 1);

// local-tex/compile.mjs
var import_node_child_process = require("node:child_process");
var import_node_fs = require("node:fs");
var import_promises = require("node:fs/promises");
var import_node_os = __toESM(require("node:os"), 1);
var import_node_path = __toESM(require("node:path"), 1);
var import_promises2 = require("node:stream/promises");
var import_yauzl = __toESM(require_yauzl(), 1);
var windows = process.platform === "win32";
var engines = { pdflatex: "-pdf", xelatex: "-pdfxe" };
var maximumExtractedSize = 512 * 1024 * 1024;
var maximumPdfSize = 256 * 1024 * 1024;
var CompileError = class extends Error {
  constructor(message, { status = 422, log, choices } = {}) {
    super(message);
    this.status = status;
    this.log = log;
    this.choices = choices;
  }
};
async function extractProject(archive, folder) {
  const names = [];
  let totalSize = 0;
  let zip;
  try {
    zip = await import_yauzl.default.fromBufferPromise(archive, { strictFileNames: true, validateEntrySizes: true });
    if (zip.entryCount > 1e4) throw new CompileError("\u9879\u76EE ZIP \u4E2D\u7684\u6587\u4EF6\u8FC7\u591A\u3002");
    for await (const entry of zip.eachEntry()) {
      const { fileName } = entry;
      const parts = fileName.replace(/\/$/, "").split("/");
      if (parts.some((part) => /[<>:"|?*\x00-\x1f]|[ .]$/.test(part) || !part || /^(?:CON|PRN|AUX|NUL|COM[0-9¹²³]|LPT[0-9¹²³])(?:\.|$)/i.test(part))) {
        throw new CompileError("\u9879\u76EE ZIP \u5305\u542B\u65E0\u6CD5\u5B89\u5168\u89E3\u538B\u7684\u6587\u4EF6\u540D\u3002");
      }
      const mode = entry.externalFileAttributes >>> 16 & 61440;
      if (mode && mode !== 32768 && mode !== 16384) throw new CompileError("\u9879\u76EE ZIP \u542B\u6709\u7B26\u53F7\u94FE\u63A5\u6216\u7279\u6B8A\u6587\u4EF6\u3002");
      totalSize += entry.uncompressedSize;
      if (totalSize > maximumExtractedSize) throw new CompileError("\u9879\u76EE\u89E3\u538B\u540E\u8D85\u8FC7 512 MB\u3002");
      if (fileName.endsWith("/") || /\.(?:exe|com|cmd|bat|dll|so|dylib)$/i.test(fileName)) continue;
      const destination = import_node_path.default.join(folder, ...parts);
      await (0, import_promises.mkdir)(import_node_path.default.dirname(destination), { recursive: true });
      await (0, import_promises2.pipeline)(await zip.openReadStreamPromise(entry), (0, import_node_fs.createWriteStream)(destination, { flags: "wx" }));
      names.push(fileName);
    }
    return names;
  } catch (error) {
    if (error instanceof CompileError) throw error;
    throw new CompileError("\u65E0\u6CD5\u89E3\u538B\u9879\u76EE ZIP\uFF0C\u8BF7\u786E\u8BA4\u5F52\u6863\u5B8C\u6574\u4E14\u6587\u4EF6\u540D\u6CA1\u6709\u51B2\u7A81\u3002");
  } finally {
    zip?.close();
  }
}
async function selectMain(folder, names, requested) {
  const texFiles = names.filter((name) => /\.(?:tex|ltx)$/i.test(name)).sort();
  if (!texFiles.length) throw new CompileError("\u9879\u76EE ZIP \u4E2D\u6CA1\u6709 .tex \u6216 .ltx \u6587\u4EF6\u3002");
  if (requested && !texFiles.includes(requested)) {
    throw new CompileError("\u627E\u4E0D\u5230\u6307\u5B9A\u7684\u4E3B\u6587\u4EF6\uFF0C\u8BF7\u4ECE\u9879\u76EE\u4E2D\u7684 TeX \u6587\u4EF6\u9009\u62E9\u3002", { choices: texFiles });
  }
  let file = requested;
  if (!file) {
    const candidates = [];
    for (const name of texFiles) {
      const header2 = await readHeader(import_node_path.default.join(folder, name));
      if (/\\documentclass\b/.test(header2.replace(/(?<!\\)%[^\r\n]*/g, ""))) candidates.push(name);
    }
    const possible = candidates.length ? candidates : texFiles;
    file = possible.find((name) => /^main\.tex$/i.test(name)) ?? (possible.length === 1 ? possible[0] : "");
    if (!file) throw new CompileError("\u9879\u76EE\u5305\u542B\u591A\u4E2A\u53EF\u80FD\u7684\u4E3B\u6587\u4EF6\uFF0C\u8BF7\u9009\u62E9\u540E\u91CD\u65B0\u7F16\u8BD1\u3002", { choices: possible });
  }
  if (/[%!'"]/.test(file)) throw new CompileError("\u4E3B\u6587\u4EF6\u8DEF\u5F84\u4E0D\u80FD\u5305\u542B %\u3001! \u6216\u5F15\u53F7\u3002");
  const header = await readHeader(import_node_path.default.join(folder, file));
  const program = /^\s*%\s*!\s*TeX\s+(?:program|TS-program)\s*=\s*(pdflatex|xelatex|lualatex)\b/im.exec(header)?.[1];
  return { file, engine: program?.toLowerCase() ?? "pdflatex" };
}
async function readHeader(file) {
  const handle = await (0, import_promises.open)(file, "r");
  try {
    const bytes = Buffer.alloc(32 * 1024);
    const { bytesRead } = await handle.read(bytes, 0, bytes.length, 0);
    return bytes.toString("utf8", 0, bytesRead);
  } finally {
    await handle.close();
  }
}
async function findLatexmk() {
  const env = { ...process.env };
  const key = Object.keys(env).find((name) => name.toUpperCase() === "PATH") ?? "PATH";
  const directories = (env[key] ?? "").split(import_node_path.default.delimiter).map((dir) => dir.replace(/^"|"$/g, "")).filter((dir) => import_node_path.default.isAbsolute(dir));
  for (const dir of directories) {
    const binary = import_node_path.default.join(dir, windows ? "latexmk.exe" : "latexmk");
    try {
      await (0, import_promises.access)(binary, import_node_fs.constants.X_OK);
    } catch {
      continue;
    }
    env[key] = [dir, ...directories.filter((entry) => entry !== dir)].join(import_node_path.default.delimiter);
    return { binary, env };
  }
  throw new CompileError("\u6CA1\u6709\u627E\u5230 latexmk\u3002\u8BF7\u5C06 TeX Live \u6216 MiKTeX \u7684 bin \u76EE\u5F55\u52A0\u5165 PATH \u540E\u91CD\u65B0\u542F\u52A8\u672C\u5730\u52A9\u624B\u3002");
}
function runLatexmk(binary, args, cwd, env) {
  return new Promise((resolve, reject) => {
    const child = (0, import_node_child_process.spawn)(binary, args, {
      cwd,
      env,
      detached: !windows,
      windowsHide: true,
      stdio: ["ignore", "pipe", "pipe"]
    });
    let log = "";
    let timedOut = false;
    let killing;
    const capture = (stream) => stream.setEncoding("utf8").on("data", (text) => {
      log = (log + text).slice(-64e3);
    });
    capture(child.stdout);
    capture(child.stderr);
    const deadline = setTimeout(() => {
      timedOut = true;
      if (windows) {
        killing = new Promise((done) => (0, import_node_child_process.execFile)(
          import_node_path.default.join(process.env.SystemRoot || "C:\\Windows", "System32", "taskkill.exe"),
          ["/PID", String(child.pid), "/T", "/F"],
          { windowsHide: true, timeout: 5e3 },
          () => {
            if (child.exitCode === null) child.kill();
            done();
          }
        ));
      } else {
        try {
          process.kill(-child.pid, "SIGKILL");
        } catch {
          child.kill();
        }
      }
    }, 8 * 60 * 1e3);
    child.once("error", (error) => {
      clearTimeout(deadline);
      reject(new CompileError("\u65E0\u6CD5\u542F\u52A8 latexmk\u3002", { log: error.message }));
    });
    child.once("close", async (code) => {
      clearTimeout(deadline);
      await killing;
      if (timedOut) reject(new CompileError("\u672C\u5730\u7F16\u8BD1\u8D85\u8FC7 8 \u5206\u949F\uFF0C\u5DF2\u505C\u6B62\u3002", { log }));
      else if (code !== 0) reject(new CompileError("\u672C\u5730 TeX \u7F16\u8BD1\u5931\u8D25\uFF0C\u8BF7\u67E5\u770B\u65E5\u5FD7\u3002", { log }));
      else resolve(log);
    });
  });
}
async function compileProject(archive, { main, engine }) {
  if (engine !== "auto" && !Object.hasOwn(engines, engine)) throw new CompileError("\u672A\u77E5\u7684\u7F16\u8BD1\u5668\u3002", { status: 400 });
  const { binary, env } = await findLatexmk();
  const temp = import_node_path.default.resolve(import_node_os.default.tmpdir());
  const workspace = await (0, import_promises.mkdtemp)(import_node_path.default.join(temp, "overleaf-local-tex-"));
  try {
    const source = import_node_path.default.join(workspace, "source");
    await (0, import_promises.mkdir)(source);
    const names = await extractProject(archive, source);
    const selection = await selectMain(source, names, main);
    engine = engine === "auto" ? selection.engine : engine;
    if (!Object.hasOwn(engines, engine)) {
      throw new CompileError("\u9879\u76EE\u6307\u5B9A\u4E86 LuaLaTeX\u3002\u672C\u5730\u52A9\u624B\u652F\u6301 pdfLaTeX \u548C XeLaTeX\u3002");
    }
    const output = await (0, import_promises.mkdtemp)(import_node_path.default.join(source, "ols-output-"));
    const args = [
      "-norc",
      engines[engine],
      "-cd-",
      "-bibfudge-",
      "-interaction=nonstopmode",
      "-halt-on-error",
      "-file-line-error",
      "-latexoption=-no-shell-escape",
      "-jobname=output",
      "-outdir=" + import_node_path.default.basename(output)
    ];
    const log = await runLatexmk(binary, [...args, "./" + selection.file], source, {
      ...env,
      openin_any: "p",
      openout_any: "p",
      shell_escape: "f",
      NoDefaultCurrentDirectoryInExePath: "1"
    });
    const pdfPath = import_node_path.default.join(output, "output.pdf");
    const pdfFile = await (0, import_promises.open)(pdfPath, "r").catch(() => null);
    if (!pdfFile) throw new CompileError("\u672C\u5730 TeX \u6CA1\u6709\u751F\u6210 PDF\u3002", { log });
    try {
      const { size } = await pdfFile.stat();
      if (size < 5 || size > maximumPdfSize) throw new CompileError("\u751F\u6210\u7684 PDF \u4E3A\u7A7A\u6216\u8D85\u8FC7 256 MB\u3002");
      const pdf = await pdfFile.readFile();
      if (pdf.subarray(0, 5).toString("ascii") !== "%PDF-") throw new CompileError("\u672C\u5730 TeX \u7684\u8F93\u51FA\u4E0D\u662F\u6709\u6548\u7684 PDF\u3002", { log });
      return { pdf, main: selection.file, engine };
    } finally {
      await pdfFile.close();
    }
  } finally {
    if (import_node_path.default.dirname(workspace) === temp && import_node_path.default.basename(workspace).startsWith("overleaf-local-tex-")) {
      await (0, import_promises.rm)(workspace, { recursive: true, force: true, maxRetries: 4, retryDelay: 100 });
    }
  }
}

// src/local-tex-protocol.js
var localTexOrigin = "http://127.0.0.1:8766";
var maxProjectZip = 128 * 1024 * 1024;

// local-tex/server.mjs
var { hostname, host, port } = new URL(localTexOrigin);
var token = (0, import_node_crypto.randomBytes)(32).toString("hex");
var tokenBytes = Buffer.from(token);
var assets = {
  "/": ["text/html; charset=utf-8", '<!doctype html>\n<html lang="zh-CN">\n<head>\n    <meta charset="utf-8">\n    <meta name="viewport" content="width=device-width, initial-scale=1">\n    <meta name="ols-token" content="@@TOKEN@@">\n    <title>Overleaf \u672C\u5730 TeX \u7F16\u8BD1</title>\n    <link rel="stylesheet" href="/style.css">\n    <script src="/app.js" defer></script>\n</head>\n<body>\n    <main>\n        <h1>Overleaf \u672C\u5730 TeX \u7F16\u8BD1</h1>\n        <form id="compile-form">\n            <label>\u9879\u76EE ZIP <input id="archive" type="file" accept=".zip,application/zip"></label>\n            <label>\u4E3B\u6587\u4EF6 <input id="main" list="main-options" placeholder="\u81EA\u52A8\u8BC6\u522B" spellcheck="false"></label>\n            <datalist id="main-options"></datalist>\n            <label>\u7F16\u8BD1\u5668\n                <select id="engine">\n                    <option value="auto">\u81EA\u52A8</option>\n                    <option value="pdflatex">pdfLaTeX</option>\n                    <option value="xelatex">XeLaTeX</option>\n                </select>\n            </label>\n            <button id="compile" disabled>\u7F16\u8BD1</button>\n        </form>\n        <p id="status" role="status">\u4ECE Overleaf \u9879\u76EE\u9875\u70B9\u51FB\u300C\u672C\u5730\u7F16\u8BD1\u300D\uFF0C\u6216\u5728\u8FD9\u91CC\u9009\u62E9\u9879\u76EE ZIP\u3002</p>\n        <details id="log-container" hidden>\n            <summary>\u7F16\u8BD1\u65E5\u5FD7</summary>\n            <pre id="log"></pre>\n        </details>\n        <section id="result" aria-label="\u672C\u5730\u7F16\u8BD1\u7ED3\u679C" hidden>\n            <a id="download" download="output.pdf">\u4E0B\u8F7D PDF</a>\n            <iframe id="pdf" title="\u672C\u5730\u7F16\u8BD1\u7684 PDF"></iframe>\n        </section>\n    </main>\n</body>\n</html>\n'.replace("@@TOKEN@@", token)],
  "/style.css": ["text/css; charset=utf-8", ":root {\n    font: 15px/1.5 system-ui, sans-serif;\n    color-scheme: light dark;\n    color: light-dark(#182522, #e2ebe8);\n    background: light-dark(#f4f7f6, #172220);\n}\n\nbody { margin: 0; }\nmain { max-width: 1300px; margin: auto; padding: 24px clamp(16px, 4vw, 52px); }\nh1 { margin: 0 0 18px; font-size: clamp(23px, 3vw, 29px); }\nform { display: flex; flex-wrap: wrap; gap: 12px 18px; align-items: end; }\nlabel { display: grid; gap: 5px; font-weight: 600; }\ninput, select, button { box-sizing: border-box; min-height: 36px; font: inherit; }\ninput:not([type=file]), select {\n    padding: 6px 10px;\n    border: 1px solid light-dark(#8a9f96, #668278);\n    border-radius: 5px;\n}\ninput[type=file] { max-width: min(270px, 80vw); }\nbutton { border: 0; border-radius: 5px; padding: 6px 22px; color: #fff; background: #18674c; cursor: pointer; }\nbutton:disabled { opacity: .6; cursor: default; }\n#status { color: light-dark(#3b5e51, #afd3c5); }\n#status[data-error] { color: light-dark(#a02c20, #ffad9d); }\nsummary { cursor: pointer; }\npre { white-space: pre-wrap; overflow-wrap: anywhere; max-height: 270px; overflow: auto; }\n#result:not([hidden]) { display: grid; gap: 8px; }\na { color: light-dark(#165e49, #9ccdbb); width: fit-content; }\niframe {\n    box-sizing: border-box;\n    width: 100%;\n    height: max(62vh, 330px);\n    border: 1px solid light-dark(#b7c9c1, #465f56);\n    border-radius: 6px;\n    background: white;\n}\n"],
  "/app.js": ["text/javascript; charset=utf-8", `(() => {
  // src/local-tex-protocol.js
  var maxProjectZip = 128 * 1024 * 1024;
  var localTexReady = "ols-local-tex:ready";
  var localTexProject = "ols-local-tex:project";
  function isOverleafOrigin(origin2) {
    return origin2 === "https://www.overleaf.com" || origin2 === "https://overleaf.com" || origin2 === "https://cn.overleaf.com";
  }

  // local-tex/app.js
  var field = (id) => document.getElementById(id);
  var [form, picker, main, engine, compile, status, log, logContainer, result, download, pdf] = ["compile-form", "archive", "main", "engine", "compile", "status", "log", "log-container", "result", "download", "pdf"].map(field);
  var token = document.querySelector('meta[name="ols-token"]').content;
  var fragment = new URLSearchParams(location.hash.slice(1));
  var session = fragment.get("session");
  var origin = fragment.get("origin");
  var archive;
  var projectKey;
  var pdfUrl;
  var running;
  function tell(message, error = false) {
    status.textContent = message;
    status.toggleAttribute("data-error", error);
  }
  picker.addEventListener("change", () => {
    archive = picker.files[0];
    projectKey = void 0;
    main.value = "";
    engine.value = "auto";
    field("main-options").replaceChildren();
    main.placeholder = "\\u81EA\\u52A8\\u8BC6\\u522B";
    logContainer.hidden = result.hidden = true;
    pdf.removeAttribute("src");
    download.removeAttribute("href");
    if (pdfUrl) URL.revokeObjectURL(pdfUrl);
    pdfUrl = void 0;
    compile.disabled = running || !archive;
    if (archive && !running) tell(archive.name + " \\u5DF2\\u5C31\\u7EEA\\u3002");
  });
  form.addEventListener("submit", (event) => {
    event.preventDefault();
    buildPdf();
  });
  async function buildPdf() {
    if (!archive || running) return;
    if (archive.size > maxProjectZip) {
      tell("\\u9879\\u76EE ZIP \\u8D85\\u8FC7 128 MB\\u3002", true);
      return;
    }
    const options = { main: main.value.trim(), engine: engine.value };
    let chooseMain = false;
    running = true;
    compile.disabled = picker.disabled = main.disabled = engine.disabled = true;
    logContainer.hidden = true;
    tell("\\u6B63\\u5728\\u4F7F\\u7528\\u672C\\u673A TeX \\u7F16\\u8BD1\\u2026");
    try {
      const query = new URLSearchParams(options);
      const response = await fetch("/compile?" + query, {
        method: "POST",
        headers: { "Content-Type": "application/zip", "X-OLS-Token": token },
        body: archive
      });
      if (!response.ok) {
        const failure = await response.json();
        if (failure.choices) {
          const entries = failure.choices.map((file2) => new Option(file2, file2));
          field("main-options").replaceChildren(...entries);
          main.value = "";
          main.placeholder = failure.choices[0];
          chooseMain = true;
        }
        if (failure.log) {
          log.textContent = failure.log;
          logContainer.hidden = false;
          logContainer.open = true;
        }
        tell(failure.error, true);
        return;
      }
      const file = decodeURIComponent(response.headers.get("x-ols-main") ?? "");
      const compiler = response.headers.get("x-ols-engine");
      if (projectKey) {
        try {
          localStorage.setItem(projectKey, JSON.stringify(options));
        } catch {
        }
      }
      const current = URL.createObjectURL(await response.blob());
      pdf.src = download.href = current;
      download.download = file.split("/").at(-1).replace(/\\.(?:tex|ltx)$/i, ".pdf");
      if (pdfUrl) URL.revokeObjectURL(pdfUrl);
      pdfUrl = current;
      result.hidden = false;
      tell("\\u7F16\\u8BD1\\u5B8C\\u6210 \\xB7 " + compiler + " \\xB7 " + file);
    } catch {
      tell("\\u672A\\u80FD\\u8BFB\\u53D6\\u7F16\\u8BD1\\u7ED3\\u679C\\u3002\\u8BF7\\u786E\\u8BA4\\u672C\\u5730\\u52A9\\u624B\\u4ECD\\u5728\\u8FD0\\u884C\\u3002", true);
    } finally {
      running = false;
      compile.disabled = picker.disabled = main.disabled = engine.disabled = false;
      if (chooseMain) main.focus();
    }
  }
  if (/^[a-f0-9-]{36}$/i.test(session ?? "") && isOverleafOrigin(origin) && window.opener) {
    let receive = function(event) {
      const data = event.data;
      if (event.origin !== origin || event.source !== parent || data?.type !== localTexProject || data.session !== session) return;
      window.removeEventListener("message", receive);
      clearTimeout(timer);
      if (archive) return;
      if (data.error || !(data.zip instanceof ArrayBuffer) || data.zip.byteLength > maxProjectZip || !/^[a-f\\d]{24}$/i.test(data.project)) {
        tell(typeof data.error === "string" ? data.error : "\\u6536\\u5230\\u7684\\u9879\\u76EE\\u6E90\\u7801\\u65E0\\u6548\\u3002", true);
        return;
      }
      archive = new Blob([data.zip], { type: "application/zip" });
      projectKey = "overleaf-local-tex:" + origin + "/" + data.project;
      try {
        const stored = JSON.parse(localStorage.getItem(projectKey));
        if (typeof stored?.main === "string") main.value = stored.main;
        if (Array.from(engine.options).some((option) => option.value === stored?.engine)) engine.value = stored.engine;
      } catch {
      }
      buildPdf();
    };
    const parent = window.opener;
    const timer = setTimeout(() => {
      window.removeEventListener("message", receive);
      if (!archive) tell("\\u672A\\u6536\\u5230\\u9879\\u76EE\\u6E90\\u7801\\u3002\\u53EF\\u4EE5\\u91CD\\u65B0\\u70B9\\u51FB Overleaf \\u7684\\u672C\\u5730\\u7F16\\u8BD1\\uFF0C\\u6216\\u9009\\u62E9\\u9879\\u76EE ZIP\\u3002", true);
    }, 125e3);
    tell("\\u6B63\\u5728\\u63A5\\u6536 Overleaf \\u9879\\u76EE\\u6E90\\u7801\\u2026");
    window.addEventListener("message", receive);
    parent.postMessage({ type: localTexReady, session }, origin);
  } else if (session) {
    tell("\\u6D4F\\u89C8\\u5668\\u672A\\u8FDE\\u63A5\\u5230 Overleaf \\u9879\\u76EE\\u9875\\u3002\\u53EF\\u4EE5\\u9009\\u62E9\\u4E0B\\u8F7D\\u7684\\u9879\\u76EE ZIP\\u3002", true);
  }
})();
`]
};
var compiling = false;
function sendJson(response, status, payload) {
  response.writeHead(status, { "Content-Type": "application/json; charset=utf-8" });
  response.end(JSON.stringify(payload));
}
async function readArchive(request) {
  if (Number(request.headers["content-length"]) > maxProjectZip) {
    throw new CompileError("\u9879\u76EE ZIP \u8D85\u8FC7 128 MB\u3002", { status: 413 });
  }
  const chunks = [];
  let size = 0;
  for await (const chunk of request.iterator({ destroyOnReturn: false })) {
    size += chunk.length;
    if (size > maxProjectZip) throw new CompileError("\u9879\u76EE ZIP \u8D85\u8FC7 128 MB\u3002", { status: 413 });
    chunks.push(chunk);
  }
  return Buffer.concat(chunks, size);
}
var server = import_node_http.default.createServer({ requestTimeout: 12e4, headersTimeout: 15e3 }, async (request, response) => {
  response.setHeader("Cache-Control", "no-store");
  response.setHeader("X-Content-Type-Options", "nosniff");
  response.setHeader("Cross-Origin-Resource-Policy", "same-origin");
  response.setHeader("Referrer-Policy", "no-referrer");
  response.setHeader("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-src blob:; frame-ancestors 'none'; base-uri 'none'; form-action 'self'");
  if (request.headers.host !== host) {
    sendJson(response, 403, { error: "\u8BF7\u4F7F\u7528\u672C\u5730\u52A9\u624B\u7684\u56DE\u73AF\u5730\u5740\u3002" });
    return;
  }
  let url;
  try {
    url = new URL(request.url, localTexOrigin);
  } catch {
    sendJson(response, 400, { error: "\u65E0\u6548\u7684\u672C\u5730\u8BF7\u6C42\u5730\u5740\u3002" });
    return;
  }
  if (url.origin !== localTexOrigin) {
    sendJson(response, 403, { error: "\u8BF7\u4F7F\u7528\u672C\u5730\u52A9\u624B\u7684\u56DE\u73AF\u5730\u5740\u3002" });
    return;
  }
  const asset = Object.hasOwn(assets, url.pathname) && assets[url.pathname];
  if (request.method === "GET" && asset) {
    response.writeHead(200, { "Content-Type": asset[0] });
    response.end(asset[1]);
    return;
  }
  if (request.method !== "POST" || url.pathname !== "/compile") {
    sendJson(response, 404, { error: "\u6CA1\u6709\u6B64\u5730\u5740\u3002" });
    return;
  }
  const supplied = Buffer.from(request.headers["x-ols-token"] ?? "");
  if (request.headers.origin !== localTexOrigin || supplied.length !== tokenBytes.length || !(0, import_node_crypto.timingSafeEqual)(supplied, tokenBytes)) {
    sendJson(response, 403, { error: "\u8BF7\u4ECE\u672C\u5730\u7F16\u8BD1\u52A9\u624B\u9875\u9762\u53D1\u8D77\u7F16\u8BD1\u3002" });
    return;
  }
  if (request.headers["content-type"] !== "application/zip") {
    sendJson(response, 415, { error: "\u8BF7\u9009\u62E9 Overleaf \u9879\u76EE ZIP\u3002" });
    return;
  }
  if (compiling) {
    sendJson(response, 409, { error: "\u672C\u5730\u52A9\u624B\u6B63\u5728\u7F16\u8BD1\u53E6\u4E00\u4EFD\u9879\u76EE\u3002\u8BF7\u7A0D\u5019\u518D\u8BD5\u3002" });
    return;
  }
  compiling = true;
  try {
    const archive = await readArchive(request);
    const { pdf, main, engine } = await compileProject(archive, {
      main: url.searchParams.get("main") ?? "",
      engine: url.searchParams.get("engine") ?? "auto"
    });
    response.writeHead(200, {
      "Content-Type": "application/pdf",
      "Content-Length": pdf.length,
      "X-OLS-Main": encodeURIComponent(main),
      "X-OLS-Engine": engine
    });
    response.end(pdf);
  } catch (error) {
    if (error instanceof CompileError) {
      if (error.status === 413) {
        response.setHeader("Connection", "close");
        request.resume();
      }
      sendJson(response, error.status, { error: error.message, log: error.log, choices: error.choices });
    } else {
      console.error(error.message);
      sendJson(response, 500, { error: "\u672C\u5730\u52A9\u624B\u9047\u5230\u9519\u8BEF\u3002\u8BF7\u68C0\u67E5\u542F\u52A8\u7A97\u53E3\u7684\u8F93\u51FA\u3002" });
    }
  } finally {
    compiling = false;
  }
});
server.once("error", (error) => {
  console.error(error.code === "EADDRINUSE" ? "\u672C\u5730\u7F16\u8BD1\u7AEF\u53E3 " + port + " \u5DF2\u88AB\u5360\u7528\u3002" : error.message);
  process.exitCode = 1;
});
server.listen(Number(port), hostname, () => console.log("Overleaf \u672C\u5730 TeX \u7F16\u8BD1\u52A9\u624B\uFF1A" + localTexOrigin));
