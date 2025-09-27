using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

public interface IFileStorage : IDisposable
{
    Guid SaveFile(Stream FileStream);
    Guid SaveFile(byte[] FileData);
    Guid SaveFile(string Base64);
    Task<Guid> SaveFileAsync(Stream FileStream, CancellationToken CancellationToken = default);
    Task<Guid> SaveFileAsync(byte[] FileData, CancellationToken CancellationToken = default);
    Task<Guid> SaveFileAsync(string Base64, CancellationToken CancellationToken = default);
    Stream GetFile(Guid FileId);
    Task<Stream> GetFileAsync(Guid FileId, CancellationToken CancellationToken = default);
    byte[] GetFileBytes(Guid FileId);
    Task<byte[]> GetFileBytesAsync(Guid FileId, CancellationToken CancellationToken = default);
    void ForceCleanup();
    void ForceCleanupOldFiles(int HoursAgo = 24);
    void RebuildIndexManually();
    void TestFileDeletion(string FileId);
    void CleanupOrphanedReferences();
}