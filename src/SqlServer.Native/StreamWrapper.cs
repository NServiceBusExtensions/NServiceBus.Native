using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

class StreamWrapper :
    Stream
{
    Stream inner;

    public StreamWrapper(long length, Stream inner)
    {
        Guard.AgainstNull(inner, nameof(inner));
        this.inner = inner;
        Length = length;
    }

    public override void EndWrite(IAsyncResult asyncResult)
    {
        inner.EndWrite(asyncResult);
    }

    public override void Flush()
    {
        inner.Flush();
    }

    public override Task FlushAsync(CancellationToken cancellation)
    {
        return inner.FlushAsync(cancellation);
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellation)
    {
        return inner.ReadAsync(buffer, offset, count, cancellation);
    }

    public override int ReadByte()
    {
        return inner.ReadByte();
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        return inner.Seek(offset, origin);
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        return inner.Read(buffer, offset, count);
    }

    public override bool CanRead => inner.CanRead;
    public override bool CanSeek => inner.CanSeek;
    public override bool CanTimeout => inner.CanTimeout;
    public override bool CanWrite => false;

    public override long Length { get; }
    public override int ReadTimeout => inner.ReadTimeout;

    public override long Position
    {
        get => inner.Position;
        set => inner.Position = value;
    }

    public override IAsyncResult BeginRead(byte[] buffer, int offset, int count, AsyncCallback? callback, object? state)
    {
        return inner.BeginRead(buffer, offset, count, callback, state);
    }

    public override int Read(Span<byte> buffer)
    {
        return inner.Read(buffer);
    }

    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        return inner.ReadAsync(buffer, cancellationToken);
    }

    public override void CopyTo(Stream destination, int bufferSize)
    {
        inner.CopyTo(destination, bufferSize);
    }

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        throw new NotImplementedException();
    }

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public override void Close()
    {
        inner.Close();
        base.Close();
    }

    public override Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellation)
    {
        return inner.CopyToAsync(destination, bufferSize, cancellation);
    }

    public override int EndRead(IAsyncResult asyncResult)
    {
        return inner.EndRead(asyncResult);
    }

    public override bool Equals(object? obj)
    {
        return inner.Equals(obj);
    }

    public override int GetHashCode()
    {
        return inner.GetHashCode();
    }

    public override string? ToString()
    {
        return inner.ToString();
    }

    public override void SetLength(long value)
    {
        throw new NotImplementedException();
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        throw new NotImplementedException();
    }

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellation)
    {
        throw new NotImplementedException();
    }

    public override void WriteByte(byte value)
    {
        throw new NotImplementedException();
    }

    public override int WriteTimeout => throw new NotImplementedException();

    public override IAsyncResult BeginWrite(byte[] buffer, int offset, int count, AsyncCallback? callback, object? state)
    {
        throw new NotImplementedException();
    }
}