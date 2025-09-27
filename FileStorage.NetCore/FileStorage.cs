using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Linq;
using System.Buffers;

public sealed class FileStorage : IFileStorage
{
    private readonly string _StoragePath;
    private readonly uint _DeleteEveryMinutes;
    private readonly ConcurrentDictionary<Guid, int> _ActiveRefs = new();
    private PeriodicTimer _oCleanupTimer;
    private readonly ManualResetEventSlim _CleanupCompleted = new(true);
    private readonly object _IndexLock = new();
    private readonly double _FreeSpaceBufferRatio = 0.1;
    private volatile int _CleanupRunning;
    private volatile int _Disposed;
    private static readonly Random _Random = Random.Shared;
    private const int _DefaultBufferSize = 8192;
    private const long _MaxFileSize = 100 * 1024 * 1024;
    private readonly DriveInfo _DriveInfo;
    private long _LastDiskCheckTime;
    private long _LastFreeSpace;
    private readonly string _LogFilePath;
    private readonly string _IndexFilePath;
    private Dictionary<string, FileIndexEntry> _FileIndex = new();
    private DateTime _LastIndexRebuild = DateTime.MinValue;
    private const int _MaxIndexEntries = 10000;
    private const int _IndexRebuildHours = 24;
    private readonly JsonSerializerOptions _JsonSettings;
    private readonly uint _MaxActiveRefMinutes;
    private readonly object _LogRotationLock = new();
    private long _CurrentLogFileSize = 0;
    private int _LogEntriesSinceLastCheck = 0;
    private const long _LogRotationThreshold = 5 * 1024 * 1024;
    private const int _LogCheckInterval = 100;
    private int _RotationCount = 0;
    private readonly int _MaxKeepBackupFile = 5;
    private readonly int _BackupFileRotationCleanUp = 10;

    private class FileIndexEntry
    {
        public string FileId { get; set; }
        public DateTime CreateDate { get; set; }
    }

    private class ReferenceCountedFileStream : FileStream
    {
        private readonly FileStorage _oStorage;
        private readonly Guid _gFileId;
        private int _bDisposed;
        public ReferenceCountedFileStream(FileStorage oStorage, Guid gFileId, string sPath, FileMode oMode, FileAccess oAccess, FileShare oShare, int iBufferSize, FileOptions oOptions)
            : base(sPath, oMode, oAccess, oShare, iBufferSize, oOptions)
        {
            _oStorage = oStorage;
            _gFileId = gFileId;
            _oStorage.IncrementRef(_gFileId);
        }
        protected override void Dispose(bool bDisposing)
        {
            if (Interlocked.Exchange(ref _bDisposed, 1) == 0)
            {
                if (bDisposing)
                {
                    _oStorage.DecrementRef(_gFileId);
                }
                base.Dispose(bDisposing);
            }
        }
    }

    public FileStorage(string sPath, uint iDeleteEveryMinute = 60)
    {
        if (string.IsNullOrWhiteSpace(sPath))
            throw new ArgumentNullException(nameof(sPath));
        try
        {
            _StoragePath = Path.GetFullPath(sPath);
            if (!Path.IsPathRooted(_StoragePath))
                throw new ArgumentException("Path must be rooted", nameof(sPath));
        }
        catch (Exception ex) when (ex is ArgumentException || ex is PathTooLongException || ex is NotSupportedException)
        {
            throw new ArgumentException("Invalid storage path", nameof(sPath), ex);
        }
        Directory.CreateDirectory(_StoragePath);
        _LogFilePath = Path.Combine(_StoragePath, "FileStorageLogs.log");
        _IndexFilePath = Path.Combine(_StoragePath, "FileIndex.json");
        try
        {
            if (File.Exists(_LogFilePath))
            {
                var oFileInfo = new FileInfo(_LogFilePath);
                Interlocked.Exchange(ref _CurrentLogFileSize, oFileInfo.Length);
                if (oFileInfo.Length > _LogRotationThreshold)
                {
                    CheckAndRotateLog();
                }
            }
            else
            {
                Interlocked.Exchange(ref _CurrentLogFileSize, 0);
            }
        }
        catch
        {
            Interlocked.Exchange(ref _CurrentLogFileSize, 0);
        }
        _DeleteEveryMinutes = iDeleteEveryMinute;
        _MaxActiveRefMinutes = (uint)(iDeleteEveryMinute * 2);
        _DriveInfo = new DriveInfo(Path.GetPathRoot(_StoragePath));
        _JsonSettings = new JsonSerializerOptions
        {
            WriteIndented = true,
            Converters = { new JsonStringEnumConverter() },
            Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
        };
        CheckPermissions();
        CleanupTempIndexFiles();
        LoadOrRebuildIndex();
        var oDelay = TimeSpan.FromMinutes(iDeleteEveryMinute);
        _oCleanupTimer = new PeriodicTimer(TimeSpan.FromMilliseconds(30000));
        _ = RunCleanupTimerAsync(oDelay);
        LogMessage($"FileStorage initialized. Path: {_StoragePath}, Cleanup interval: {iDeleteEveryMinute} minute");
    }

    private async Task RunCleanupTimerAsync(TimeSpan oInterval)
    {
        while (await _oCleanupTimer.WaitForNextTickAsync())
        {
            OnCleanupTimer(null);
            _oCleanupTimer.Period = oInterval;
        }
    }

    private void LoadOrRebuildIndex()
    {
        lock (_IndexLock)
        {
            try
            {
                if (File.Exists(_IndexFilePath))
                {
                    string sJson = File.ReadAllText(_IndexFilePath);
                    var loadedIndex = JsonSerializer.Deserialize<List<FileIndexEntry>>(sJson, _JsonSettings) ?? new List<FileIndexEntry>();
                    _FileIndex = loadedIndex.ToDictionary(e => e.FileId);
                    if (_FileIndex.Count > _MaxIndexEntries ||
                        (DateTime.UtcNow - _LastIndexRebuild) > TimeSpan.FromHours(_IndexRebuildHours))
                    {
                        LogMessage($"Index needs rebuild. Entries: {_FileIndex.Count}, Last rebuild: {_LastIndexRebuild}");
                        RebuildIndex();
                    }
                    else
                    {
                        LogMessage($"Index loaded successfully. Entries: {_FileIndex.Count}");
                    }
                }
                else
                {
                    LogMessage("Index file not found. Rebuilding from existing files");
                    RebuildIndex();
                }
            }
            catch (Exception ex)
            {
                LogMessage($"Failed to load index: {ex}. Rebuilding...");
                RebuildIndex();
            }
        }
    }

    private void RebuildIndex()
    {
        lock (_IndexLock)
        {
            try
            {
                var newIndex = new Dictionary<string, FileIndexEntry>();
                var oDirInfo = new DirectoryInfo(_StoragePath);
                foreach (var oFileInfo in oDirInfo.EnumerateFiles("*.dat"))
                {
                    string sFileName = Path.GetFileNameWithoutExtension(oFileInfo.Name);
                    if (Guid.TryParseExact(sFileName, "N", out Guid gGuid))
                    {
                        DateTime dtCreated = GetFileCreationTime(oFileInfo);
                        newIndex[gGuid.ToString("N")] = new FileIndexEntry { FileId = gGuid.ToString("N"), CreateDate = dtCreated };
                    }
                }
                _FileIndex = newIndex;
                SaveIndex();
                _LastIndexRebuild = DateTime.UtcNow;
                LogMessage($"Index rebuilt successfully. Entries: {_FileIndex.Count}");
            }
            catch (Exception ex)
            {
                LogMessage($"Failed to rebuild index: {ex}");
            }
        }
    }

    private void SaveIndex()
    {
        lock (_IndexLock)
        {
            const int iMaxRetries = 3;
            const int iBaseDelayMs = 100;
            string sTempPath = Path.Combine(_StoragePath, "FileIndex.tmp");
            for (int iAttempt = 1; iAttempt <= iMaxRetries; iAttempt++)
            {
                try
                {
                    if (File.Exists(sTempPath))
                    {
                        try { File.Delete(sTempPath); }
                        catch { }
                    }
                    string sJson = JsonSerializer.Serialize(_FileIndex.Values.ToList(), _JsonSettings);
                    File.WriteAllText(sTempPath, sJson, Encoding.UTF8);
                    if (File.Exists(_IndexFilePath))
                    {
                        File.Replace(sTempPath, _IndexFilePath, null);
                    }
                    else
                    {
                        File.Move(sTempPath, _IndexFilePath);
                    }
                    return;
                }
                catch (Exception ex) when (iAttempt < iMaxRetries)
                {
                    LogMessage($"Attempt {iAttempt} to save index failed: {ex.Message}. Retrying...");
                    Thread.Sleep(iBaseDelayMs * iAttempt);
                }
                catch (Exception ex)
                {
                    LogMessage($"Failed to save index after {iMaxRetries} attempts: {ex}");
                    try
                    {
                        string sBackupPath = Path.Combine(_StoragePath, "FileIndex_backup.json");
                        string sJson = JsonSerializer.Serialize(_FileIndex.Values.ToList(), _JsonSettings);
                        File.WriteAllText(sBackupPath, sJson, Encoding.UTF8);
                        if (File.Exists(_IndexFilePath))
                        {
                            File.Delete(_IndexFilePath);
                        }
                        File.Move(sBackupPath, _IndexFilePath);
                        LogMessage("Index saved using fallback method");
                        return;
                    }
                    catch (Exception fallbackEx)
                    {
                        LogMessage($"Fallback save also failed: {fallbackEx}");
                    }
                }
                finally
                {
                    if (File.Exists(sTempPath))
                    {
                        try { File.Delete(sTempPath); }
                        catch { }
                    }
                }
            }
        }
    }

    private void AddToIndex(Guid gFileId, DateTime dtCreated)
    {
        lock (_IndexLock)
        {
            dtCreated = dtCreated.ToUniversalTime();
            _FileIndex[gFileId.ToString("N")] = new FileIndexEntry { FileId = gFileId.ToString("N"), CreateDate = dtCreated };
            SaveIndex();
        }
    }

    private void RemoveFromIndex(Guid gFileId)
    {
        lock (_IndexLock)
        {
            _FileIndex.Remove(gFileId.ToString("N"));
            SaveIndex();
        }
    }

    private void RemoveStaleEntries()
    {
        lock (_IndexLock)
        {
            var lstEntriesToRemove = new List<string>();
            foreach (var oEntry in _FileIndex)
            {
                string sFilePath = Path.Combine(_StoragePath, oEntry.Key + ".dat");
                if (!File.Exists(sFilePath))
                {
                    lstEntriesToRemove.Add(oEntry.Key);
                }
            }
            foreach (var sFileId in lstEntriesToRemove)
            {
                _FileIndex.Remove(sFileId);
            }
            if (lstEntriesToRemove.Count > 0)
            {
                SaveIndex();
                LogMessage($"Removed {lstEntriesToRemove.Count} stale entries from index");
            }
        }
    }

    private void CleanupTempIndexFiles()
    {
        try
        {
            var oDirInfo = new DirectoryInfo(_StoragePath);
            var sTempFiles = new[] { "FileIndex.tmp", "FileIndex_backup.json" };
            foreach (var sPattern in sTempFiles)
            {
                var oFiles = oDirInfo.GetFiles(sPattern);
                foreach (var oFile in oFiles)
                {
                    try
                    {
                        if (DateTime.UtcNow - oFile.CreationTimeUtc > TimeSpan.FromHours(1))
                        {
                            oFile.Delete();
                            LogMessage($"Deleted old temp file: {oFile.Name}");
                        }
                    }
                    catch (Exception ex)
                    {
                        LogMessage($"Failed to delete temp file {oFile.Name}: {ex}");
                    }
                }
            }
            foreach (var oFile in oDirInfo.GetFiles("FileIndex_*.tmp"))
            {
                try
                {
                    if (DateTime.UtcNow - oFile.CreationTimeUtc > TimeSpan.FromHours(1))
                    {
                        oFile.Delete();
                        LogMessage($"Deleted old random temp file: {oFile.Name}");
                    }
                }
                catch (Exception ex)
                {
                    LogMessage($"Failed to delete random temp file {oFile.Name}: {ex}");
                }
            }
        }
        catch (Exception ex)
        {
            LogMessage($"Failed to cleanup temp files: {ex}");
        }
    }

    private void CheckPermissions()
    {
        try
        {
            string sTestFile = Path.Combine(_StoragePath, "permission_test.tmp");
            File.WriteAllText(sTestFile, "test");
            File.Delete(sTestFile);
            LogMessage("Permission check passed successfully");
        }
        catch (Exception ex)
        {
            throw new Exception(
                $"The application does not have write/delete permissions on the storage path: {_StoragePath} " +
                $"Please grant 'Modify' permissions. Error: {ex.Message}", ex);
        }
    }

    public Guid SaveFile(Stream oFileStream) => SaveFileCore(oFileStream);
    public Guid SaveFile(byte[] oFileData) => SaveFileCore(new MemoryStream(oFileData), oFileData.Length);
    public Guid SaveFile(string sBase64) => SaveFile(Convert.FromBase64String(sBase64));
    public Task<Guid> SaveFileAsync(Stream oFileStream, CancellationToken oCt = default) => SaveFileCoreAsync(oFileStream, null, oCt);
    public Task<Guid> SaveFileAsync(byte[] oFileData, CancellationToken oCt = default) => SaveFileCoreAsync(new MemoryStream(oFileData), oFileData.Length, oCt);
    public Task<Guid> SaveFileAsync(string sBase64, CancellationToken oCt = default) => SaveFileAsync(Convert.FromBase64String(sBase64), oCt);

    public Stream GetFile(Guid gFileId)
    {
        CheckDisposed();
        var sFilePath = GetFilePath(gFileId);
        try
        {
            return new ReferenceCountedFileStream(
                this,
                gFileId,
                sFilePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read | FileShare.Delete,
                _DefaultBufferSize,
                FileOptions.RandomAccess | FileOptions.SequentialScan);
        }
        catch (FileNotFoundException ex)
        {
            throw new FileNotFoundException("File not found", sFilePath, ex);
        }
    }

    public async Task<Stream> GetFileAsync(Guid gFileId, CancellationToken oCt = default)
    {
        CheckDisposed();
        oCt.ThrowIfCancellationRequested();
        var sFilePath = GetFilePath(gFileId);
        try
        {
            return new ReferenceCountedFileStream(
                this,
                gFileId,
                sFilePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read | FileShare.Delete,
                _DefaultBufferSize,
                FileOptions.Asynchronous | FileOptions.SequentialScan);
        }
        catch (FileNotFoundException ex)
        {
            throw new FileNotFoundException("File not found", sFilePath, ex);
        }
    }

    public byte[] GetFileBytes(Guid gFileId)
    {
        CheckDisposed();
        var sFilePath = GetFilePath(gFileId);
        var lFileSize = new FileInfo(sFilePath).Length;
        if (lFileSize > _MaxFileSize)
            throw new IOException($"File is too large to load into memory (max {_MaxFileSize} bytes)");
        IncrementRef(gFileId);
        try
        {
            return ExecuteWithRetry(() =>
            {
                int iBufferSize = GetOptimizedBufferSize(lFileSize);
                using (var oStream = new FileStream(
                    sFilePath,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.Read | FileShare.Delete,
                    iBufferSize,
                    FileOptions.SequentialScan))
                {
                    return ReadAllBytes(oStream, lFileSize);
                }
            });
        }
        finally
        {
            DecrementRef(gFileId);
        }
    }

    public async Task<byte[]> GetFileBytesAsync(Guid gFileId, CancellationToken oCt = default)
    {
        CheckDisposed();
        var sFilePath = GetFilePath(gFileId);
        var lFileSize = new FileInfo(sFilePath).Length;
        if (lFileSize > _MaxFileSize)
            throw new IOException($"File is too large to load into memory (max {_MaxFileSize} bytes)");
        IncrementRef(gFileId);
        try
        {
            return await ExecuteWithRetryAsync(async () =>
            {
                int iBufferSize = GetOptimizedBufferSize(lFileSize);
                using (var oStream = new FileStream(
                    sFilePath,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.Read | FileShare.Delete,
                    iBufferSize,
                    FileOptions.Asynchronous | FileOptions.SequentialScan))
                {
                    return await ReadAllBytesAsync(oStream, lFileSize, oCt);
                }
            }, oCt);
        }
        finally
        {
            DecrementRef(gFileId);
        }
    }

    private byte[] ReadAllBytes(FileStream stream, long fileSize)
    {
        var buffer = new byte[fileSize];
        int bytesRead = 0;
        while (bytesRead < fileSize)
        {
            int read = stream.Read(buffer, bytesRead, (int)(fileSize - bytesRead));
            if (read == 0)
                throw new IOException("File read incomplete");
            bytesRead += read;
        }
        return buffer;
    }

    private async Task<byte[]> ReadAllBytesAsync(FileStream stream, long fileSize, CancellationToken ct)
    {
        var buffer = new byte[fileSize];
        int bytesRead = 0;
        while (bytesRead < fileSize)
        {
            int read = await stream.ReadAsync(buffer.AsMemory(bytesRead, (int)(fileSize - bytesRead)), ct).ConfigureAwait(false);
            if (read == 0)
                throw new IOException("File read incomplete");
            bytesRead += read;
        }
        return buffer;
    }

    private int GetOptimizedBufferSize(long lFileSize)
    {
        if (lFileSize <= 8192) return 8192;
        if (lFileSize <= 65536) return 16384;
        if (lFileSize <= 524288) return 32768;
        return 65536;
    }

    private Guid SaveFileCore(Stream oStream, long? lKnownLength = null)
    {
        CheckDisposed();
        if (oStream == null)
            throw new ArgumentNullException(nameof(oStream));
        long lFileSize = lKnownLength ?? (oStream.CanSeek ? oStream.Length : _MaxFileSize);
        if (lFileSize > _MaxFileSize)
            throw new IOException($"File size exceeds maximum allowed size of {_MaxFileSize} bytes");
        EnsureDiskSpace(lFileSize);
        var gFileId = Guid.NewGuid();
        var sTempPath = GetTempPath(gFileId);
        var sFinalPath = GetFilePath(gFileId);
        int iBufferSize = oStream.CanSeek && lKnownLength.HasValue ? GetOptimizedBufferSize(lKnownLength.Value) : _DefaultBufferSize;
        IncrementRef(gFileId);
        try
        {
            ExecuteWithRetry(() =>
            {
                using (var oFs = new FileStream(
                    sTempPath,
                    FileMode.CreateNew,
                    FileAccess.Write,
                    FileShare.None,
                    iBufferSize,
                    FileOptions.SequentialScan))
                {
                    oStream.CopyTo(oFs, iBufferSize);
                }
                File.Move(sTempPath, sFinalPath);
                AddToIndex(gFileId, DateTime.UtcNow);
            });
        }
        catch
        {
            SafeDeleteFile(sTempPath);
            throw;
        }
        finally
        {
            DecrementRef(gFileId);
        }
        return gFileId;
    }

    private async Task<Guid> SaveFileCoreAsync(Stream oStream, long? lKnownLength, CancellationToken oCt = default)
    {
        CheckDisposed();
        if (oStream == null)
            throw new ArgumentNullException(nameof(oStream));
        long lFileSize = lKnownLength ?? (oStream.CanSeek ? oStream.Length : _MaxFileSize);
        if (lFileSize > _MaxFileSize)
            throw new IOException($"File size exceeds maximum allowed size of {_MaxFileSize} bytes");
        EnsureDiskSpace(lFileSize);
        var gFileId = Guid.NewGuid();
        var sTempPath = GetTempPath(gFileId);
        var sFinalPath = GetFilePath(gFileId);
        int iBufferSize = oStream.CanSeek && lKnownLength.HasValue
            ? GetOptimizedBufferSize(lKnownLength.Value)
            : _DefaultBufferSize;
        IncrementRef(gFileId);
        try
        {
            await ExecuteWithRetryAsync(async () =>
            {
                await using (var oFs = new FileStream(
                    sTempPath,
                    FileMode.CreateNew,
                    FileAccess.Write,
                    FileShare.None,
                    iBufferSize,
                    FileOptions.Asynchronous | FileOptions.SequentialScan))
                {
                    await oStream.CopyToAsync(oFs, iBufferSize, oCt).ConfigureAwait(false);
                }
                File.Move(sTempPath, sFinalPath);
                AddToIndex(gFileId, DateTime.UtcNow);
            }, oCt).ConfigureAwait(false);
        }
        catch
        {
            SafeDeleteFile(sTempPath);
            throw;
        }
        finally
        {
            DecrementRef(gFileId);
        }
        return gFileId;
    }

    private void OnCleanupTimer(object oState)
    {
        if (_Disposed != 0)
            return;
        if (Interlocked.CompareExchange(ref _CleanupRunning, 1, 0) != 0)
            return;
        _CleanupCompleted.Reset();
        try
        {
            LogMessage("Cleanup timer triggered");
            CleanupTempIndexFiles();
            CleanupOrphanedReferences();
            CleanupOldFiles();
        }
        catch (Exception ex)
        {
            LogMessage($"Cleanup failed: {ex}");
        }
        finally
        {
            Interlocked.Exchange(ref _CleanupRunning, 0);
            _CleanupCompleted.Set();
        }
    }

    private void CleanupOldFiles()
    {
        try
        {
            LogMessage($"Current system time (UTC): {DateTime.UtcNow}");
            LogMessage($"Current system time (Local): {DateTime.Now}");
            var dtCutoff = DateTime.UtcNow - TimeSpan.FromMinutes(_DeleteEveryMinutes);
            var dtForceDeleteCutoff = DateTime.UtcNow - TimeSpan.FromMinutes(_DeleteEveryMinutes + _MaxActiveRefMinutes);
            LogMessage($"Cleanup started. Cutoff time (UTC): {dtCutoff} (Local: {dtCutoff.ToLocalTime()})");
            LogMessage($"Force delete cutoff (UTC): {dtForceDeleteCutoff} (Local: {dtForceDeleteCutoff.ToLocalTime()})");

            RemoveStaleEntries();

            if ((DateTime.UtcNow - _LastIndexRebuild) > TimeSpan.FromHours(_IndexRebuildHours))
            {
                LogMessage("Scheduled index rebuild");
                RebuildIndex();
            }

            int iDeletedCount = 0;
            int iFailedCount = 0;

            var lstEntriesToDelete = new List<FileIndexEntry>();
            lock (_IndexLock)
            {
                LogMessage($"Processing {_FileIndex.Count} entries in index");
                foreach (var oEntry in _FileIndex.Values)
                {
                    LogMessage($"Processing file: {oEntry.FileId}");
                    LogMessage($"  File created (UTC): {oEntry.CreateDate}");
                    LogMessage($"  File created (Local): {oEntry.CreateDate.ToLocalTime()}");
                    LogMessage($"  Cutoff time (UTC): {dtCutoff}");
                    LogMessage($"  Force delete cutoff (UTC): {dtForceDeleteCutoff}");

                    bool bIsOldEnough = oEntry.CreateDate < dtCutoff;
                    bool bIsVeryOld = oEntry.CreateDate < dtForceDeleteCutoff;
                    LogMessage($"  Is old enough? {bIsOldEnough}");
                    LogMessage($"  Is very old? {bIsVeryOld}");

                    if (bIsVeryOld)
                    {
                        lstEntriesToDelete.Add(oEntry);
                        LogMessage($"  FORCE DELETION (very old): {oEntry.FileId}");
                    }
                    else if (bIsOldEnough)
                    {
                        if (Guid.TryParseExact(oEntry.FileId, "N", out Guid gFileId))
                        {
                            bool bHasActiveRef = _ActiveRefs.TryGetValue(gFileId, out int iCount) && iCount > 0;
                            LogMessage($"  Has active reference: {bHasActiveRef}");

                            if (!bHasActiveRef)
                            {
                                lstEntriesToDelete.Add(oEntry);
                                LogMessage($"  MARKED FOR DELETION: {oEntry.FileId}");
                            }
                            else
                            {
                                LogMessage($"  NOT DELETED (has active ref but not old enough for force): {oEntry.FileId}");
                            }
                        }
                        else
                        {
                            LogMessage($"  NOT DELETED (invalid GUID): {oEntry.FileId}");
                        }
                    }
                    else
                    {
                        LogMessage($"  NOT DELETED (not old enough): {oEntry.FileId}");
                    }
                }
            }

            LogMessage($"Found {lstEntriesToDelete.Count} files to delete from index");
            foreach (var oEntry in lstEntriesToDelete)
            {
                string sFilePath = Path.Combine(_StoragePath, oEntry.FileId + ".dat");
                LogMessage($"Attempting to delete: {oEntry.FileId}");
                if (DeleteFileAndRemoveFromIndex(sFilePath, oEntry))
                {
                    iDeletedCount++;
                    LogMessage($"SUCCESSFULLY deleted: {oEntry.FileId}");
                }
                else
                {
                    iFailedCount++;
                    LogMessage($"FAILED to delete: {oEntry.FileId}");
                }
            }

            var oDirInfo = new DirectoryInfo(_StoragePath);
            var allFiles = oDirInfo.GetFiles("*.dat");
            LogMessage($"Found {allFiles.Length} .dat files in directory");

            foreach (var oFileInfo in allFiles)
            {
                string sFileName = Path.GetFileNameWithoutExtension(oFileInfo.Name);
                if (Guid.TryParseExact(sFileName, "N", out Guid gFileId))
                {
                    bool bInIndex = _FileIndex.ContainsKey(gFileId.ToString("N"));

                    if (!bInIndex)
                    {
                        DateTime dtCreated = GetFileCreationTime(oFileInfo);
                        bool bIsOldEnough = dtCreated < dtCutoff;
                        bool bIsVeryOld = dtCreated < dtForceDeleteCutoff;

                        LogMessage($"File not in index: {sFileName}");
                        LogMessage($"  Created (UTC): {dtCreated}");
                        LogMessage($"  Is old enough: {bIsOldEnough}");
                        LogMessage($"  Is very old: {bIsVeryOld}");

                        if (bIsVeryOld || bIsOldEnough)
                        {
                            if (SafeDeleteFile(oFileInfo.FullName))
                            {
                                iDeletedCount++;
                                LogMessage($"Deleted old file not in index: {sFileName}");
                            }
                            else
                            {
                                iFailedCount++;
                                LogMessage($"Failed to delete file not in index: {sFileName}");
                            }
                        }
                        else
                        {
                            LogMessage($"File not in index but not old enough: {sFileName}");
                        }
                    }
                }
            }

            foreach (var oFileInfo in oDirInfo.EnumerateFiles("*.tmp"))
            {
                if (oFileInfo.Name.Equals("FileIndex.tmp", StringComparison.OrdinalIgnoreCase))
                    continue;

                if (SafeDeleteFile(oFileInfo.FullName))
                {
                    iDeletedCount++;
                    LogMessage($"Deleted temp file: {oFileInfo.Name}");
                }
                else
                {
                    iFailedCount++;
                }
            }

            LogMessage($"Cleanup completed. Deleted: {iDeletedCount}, Failed: {iFailedCount}");
        }
        catch (Exception ex)
        {
            LogMessage($"Global cleanup error: {ex}");
        }
    }

    private bool DeleteFileAndRemoveFromIndex(string sFilePath, FileIndexEntry oEntry)
    {
        if (SafeDeleteFile(sFilePath))
        {
            lock (_IndexLock)
            {
                _FileIndex.Remove(oEntry.FileId);
                SaveIndex();
            }
            return true;
        }
        return false;
    }

    private DateTime GetFileCreationTime(FileInfo oFileInfo)
    {
        try
        {
            var dtUtc = oFileInfo.CreationTimeUtc;
            LogMessage($"Got creation time from UTC: {dtUtc}");
            return dtUtc;
        }
        catch (Exception exUtc)
        {
            LogMessage($"Failed to get UTC creation time: {exUtc}");
            try
            {
                var dtLocal = oFileInfo.CreationTime;
                var dtConverted = dtLocal.ToUniversalTime();
                LogMessage($"Got creation time from Local: {dtLocal}, Converted to UTC: {dtConverted}");
                return dtConverted;
            }
            catch (Exception exLocal)
            {
                LogMessage($"Failed to get Local creation time: {exLocal}");
                try
                {
                    var dtWrite = oFileInfo.LastWriteTimeUtc;
                    LogMessage($"Falling back to LastWriteTimeUtc: {dtWrite}");
                    return dtWrite;
                }
                catch (Exception exWrite)
                {
                    LogMessage($"Failed to get LastWriteTimeUtc: {exWrite}");
                    return DateTime.UtcNow;
                }
            }
        }
    }

    private string GetFilePath(Guid gFileId) => Path.Combine(_StoragePath, gFileId.ToString("N") + ".dat");
    private string GetTempPath(Guid gFileId) => Path.Combine(_StoragePath, gFileId.ToString("N") + ".tmp");

    private void IncrementRef(Guid gFileId)
    {
        _ActiveRefs.AddOrUpdate(gFileId, 1, (key, value) => value + 1);
    }

    private void DecrementRef(Guid gFileId)
    {
        int iNewValue = _ActiveRefs.AddOrUpdate(gFileId, 0, (key, value) => value - 1);
        if (iNewValue <= 0)
        {
            _ActiveRefs.TryRemove(gFileId, out _);
        }
    }

    private T ExecuteWithRetry<T>(Func<T> oFunc, int iMaxRetries = 2, int iBaseDelay = 5)
    {
        for (int i = 0; i < iMaxRetries; i++)
        {
            try
            {
                return oFunc();
            }
            catch (Exception ex) when (IsTransientFileError(ex) && i < iMaxRetries - 1)
            {
                Thread.Sleep(iBaseDelay * (1 << i) + _Random.Next(0, 5));
            }
        }
        throw new IOException("File operation failed after retries");
    }

    private void ExecuteWithRetry(Action oAction, int iMaxRetries = 2, int iBaseDelay = 5)
    {
        ExecuteWithRetry(() =>
        {
            oAction();
            return true;
        }, iMaxRetries, iBaseDelay);
    }

    private async Task<T> ExecuteWithRetryAsync<T>(Func<Task<T>> oFunc, CancellationToken oCt = default, int iMaxRetries = 2, int iBaseDelay = 5)
    {
        for (int i = 0; i < iMaxRetries; i++)
        {
            oCt.ThrowIfCancellationRequested();
            try
            {
                return await oFunc().ConfigureAwait(false);
            }
            catch (Exception ex) when (IsTransientFileError(ex) && i < iMaxRetries - 1)
            {
                var iDelay = iBaseDelay * (1 << i) + _Random.Next(0, 5);
                await Task.Delay(iDelay, oCt).ConfigureAwait(false);
            }
        }
        throw new IOException("Async file operation failed after retries");
    }

    private async Task ExecuteWithRetryAsync(Func<Task> oAction, CancellationToken oCt = default, int iMaxRetries = 2, int iBaseDelay = 5)
    {
        await ExecuteWithRetryAsync(async () =>
        {
            await oAction().ConfigureAwait(false);
            return true;
        }, oCt, iMaxRetries, iBaseDelay).ConfigureAwait(false);
    }

    private void EnsureDiskSpace(long lRequiredSpace)
    {
        long lNow = Stopwatch.GetTimestamp();
        if (lNow - _LastDiskCheckTime < TimeSpan.TicksPerSecond * 5)
        {
            if (_LastFreeSpace >= lRequiredSpace * (1 + _FreeSpaceBufferRatio))
                return;
        }
        try
        {
            long lCurrentFreeSpace = _DriveInfo.AvailableFreeSpace;
            Interlocked.Exchange(ref _LastFreeSpace, lCurrentFreeSpace);
            Interlocked.Exchange(ref _LastDiskCheckTime, lNow);
            long lRequiredWithBuffer = (long)(lRequiredSpace * (1 + _FreeSpaceBufferRatio));
            if (lCurrentFreeSpace < lRequiredWithBuffer)
                throw new IOException("Not enough disk space available");
        }
        catch (Exception ex)
        {
            LogMessage($"Failed to check disk space: {ex}");
            throw new IOException("Failed to check disk space", ex);
        }
    }

    private bool SafeDeleteFile(string sPath)
    {
        const int iMaxRetries = 3;
        const int iBaseDelayMs = 100;
        for (int i = 0; i < iMaxRetries; i++)
        {
            try
            {
                if (!File.Exists(sPath))
                    return true;
                var oFileInfo = new FileInfo(sPath);
                if (oFileInfo.IsReadOnly)
                {
                    oFileInfo.IsReadOnly = false;
                    LogMessage($"Removed read-only attribute from file: {sPath}");
                }
                File.Delete(sPath);
                LogMessage($"Successfully deleted file: {sPath}");
                return true;
            }
            catch (Exception ex) when (ex is FileNotFoundException || ex is DirectoryNotFoundException)
            {
                return true;
            }
            catch (Exception ex) when (i < iMaxRetries - 1 && (ex is IOException || ex is UnauthorizedAccessException))
            {
                LogMessage($"Attempt {i + 1} to delete file '{sPath}' failed: {ex.Message}. Retrying in {iBaseDelayMs * (i + 1)}ms...");
                Thread.Sleep(iBaseDelayMs * (i + 1));
            }
            catch (Exception ex)
            {
                LogMessage($"Critical error deleting file '{sPath}': {ex}");
                return false;
            }
        }
        LogMessage($"Failed to delete file '{sPath}' after {iMaxRetries} attempts");
        return false;
    }

    private void CheckDisposed()
    {
        if (_Disposed != 0)
            throw new ObjectDisposedException("FileStorage");
    }

    private bool IsTransientFileError(Exception oEx)
    {
        return oEx is IOException || oEx is UnauthorizedAccessException;
    }

    //private void LogMessage(string sMessage)
    //{
    //    string sLogEntry = $"UtcTime:{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss}=>LocalTime:{DateTime.Now:yyyy-MM-dd HH:mm:ss}: {sMessage}{Environment.NewLine}";
    //    int EntrySize = Encoding.UTF8.GetByteCount(sLogEntry);
    //    if (Interlocked.Increment(ref _LogEntriesSinceLastCheck) >= _LogCheckInterval)
    //    {
    //        CheckAndRotateLog();
    //    }
    //    const int MaxRetries = 2;
    //    const int iBaseDelayMs = 100;
    //    for (int iAttempt = 0; iAttempt < MaxRetries; iAttempt++)
    //    {
    //        try
    //        {
    //            using var oFs = new FileStream(
    //                _LogFilePath,
    //                FileMode.Append,
    //                FileAccess.Write,
    //                FileShare.Read,
    //                bufferSize: 8192,
    //                options: FileOptions.SequentialScan);
    //            using var oWriter = new StreamWriter(oFs, Encoding.UTF8);
    //            oWriter.Write(sLogEntry);
    //            oWriter.Flush();
    //            Interlocked.Add(ref _CurrentLogFileSize, EntrySize);
    //            return;
    //        }
    //        catch (Exception ex)
    //        {
    //            bool bIsFileNotFound = ex is FileNotFoundException || (ex is IOException && ex.Message.Contains("Could not find")) ||
    //                                  (ex is IOException && ex.Message.Contains("The system cannot find the file"));

    //            if (bIsFileNotFound)
    //            {
    //                try
    //                {
    //                    string NewLogHeader = $"Log File Created At UtcTime {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} LocalTime {DateTime.Now:yyyy-MM-dd HH:mm:ss}{Environment.NewLine}";
    //                    File.WriteAllText(_LogFilePath, NewLogHeader, Encoding.UTF8);
    //                    Interlocked.Exchange(ref _CurrentLogFileSize, Encoding.UTF8.GetByteCount(NewLogHeader));
    //                    Thread.Sleep(iBaseDelayMs);
    //                }
    //                catch
    //                {
    //                    return;
    //                }
    //            }
    //            else if (iAttempt == MaxRetries - 1)
    //            {
    //                return;
    //            }
    //            else
    //            {
    //                Thread.Sleep(iBaseDelayMs * (iAttempt + 1));
    //            }
    //        }
    //    }
    //}
    //private void LogMessage(string sMessage)
    //{
    //    string sLogEntry = $"UtcTime:{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss}=>LocalTime:{DateTime.Now:yyyy-MM-dd HH:mm:ss}: {sMessage}{Environment.NewLine}";
    //    int EntrySize = Encoding.UTF8.GetByteCount(sLogEntry);
    //    if (Interlocked.Increment(ref _LogEntriesSinceLastCheck) >= _LogCheckInterval)
    //    {
    //        CheckAndRotateLog();
    //    }
    //    const int MaxRetries = 2;
    //    const int iBaseDelayMs = 100;
    //    int iAttempt = 0;
    //Retry:
    //    try
    //    {
    //        using var oFs = new FileStream(
    //            _LogFilePath,
    //            FileMode.Append,
    //            FileAccess.Write,
    //            FileShare.Read,
    //            bufferSize: 8192,
    //            options: FileOptions.SequentialScan);
    //        using var oWriter = new StreamWriter(oFs, Encoding.UTF8);
    //        oWriter.Write(sLogEntry);
    //        oWriter.Flush();
    //        Interlocked.Add(ref _CurrentLogFileSize, EntrySize);
    //        return;
    //    }
    //    catch (Exception oEx)
    //    {
    //        bool bIsFileNotFound = oEx is FileNotFoundException || (oEx is IOException && oEx.Message.Contains("Could not find")) || (oEx is IOException && oEx.Message.Contains("The system cannot find the file"));
    //        if (bIsFileNotFound)
    //        {
    //            try
    //            {
    //                string NewLogHeader = $"Log File Created At UtcTime {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} LocalTime {DateTime.Now:yyyy-MM-dd HH:mm:ss}{Environment.NewLine}";
    //                File.WriteAllText(_LogFilePath, NewLogHeader, Encoding.UTF8);
    //                Interlocked.Exchange(ref _CurrentLogFileSize, Encoding.UTF8.GetByteCount(NewLogHeader));
    //                Thread.Sleep(iBaseDelayMs);
    //            }
    //            catch
    //            {
    //                return;
    //            }
    //        }
    //        iAttempt++;
    //        if (iAttempt >= MaxRetries)
    //        {
    //            return;
    //        }

    //        if (!bIsFileNotFound)
    //        {
    //            Thread.Sleep(iBaseDelayMs * iAttempt);
    //        }
    //        goto Retry;
    //    }
    //}
    private void LogMessage(string sMessage)
    {
        string sLogEntry = $"UtcTime:{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss}=>LocalTime:{DateTime.Now:yyyy-MM-dd HH:mm:ss}: {sMessage}{Environment.NewLine}";
        int EntrySize = Encoding.UTF8.GetByteCount(sLogEntry);
        if (Interlocked.Increment(ref _LogEntriesSinceLastCheck) >= _LogCheckInterval)
        {
            CheckAndRotateLog();
        }
        const int MaxRetries = 2;
        const int iBaseDelayMs = 100;
        void TryWriteLog(int iAttempt)
        {
            try
            {
                using var oFs = new FileStream(
                    _LogFilePath,
                    FileMode.Append,
                    FileAccess.Write,
                    FileShare.Read,
                    bufferSize: 8192,
                    options: FileOptions.SequentialScan);
                using var oWriter = new StreamWriter(oFs, Encoding.UTF8);
                oWriter.Write(sLogEntry);
                oWriter.Flush();
                Interlocked.Add(ref _CurrentLogFileSize, EntrySize);
                return;
            }
            catch (Exception oEx)
            {
                bool bIsFileNotFound = oEx is FileNotFoundException || (oEx is IOException && oEx.Message.Contains("Could not find")) || (oEx is IOException && oEx.Message.Contains("The system cannot find the file"));
                if (bIsFileNotFound)
                {
                    try
                    {
                        string NewLogHeader = $"Log File Created At UtcTime {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} LocalTime {DateTime.Now:yyyy-MM-dd HH:mm:ss}{Environment.NewLine}";
                        File.WriteAllText(_LogFilePath, NewLogHeader, Encoding.UTF8);
                        Interlocked.Exchange(ref _CurrentLogFileSize, Encoding.UTF8.GetByteCount(NewLogHeader));
                        Thread.Sleep(iBaseDelayMs);
                    }
                    catch
                    {
                        return;
                    }
                }
                if (iAttempt >= MaxRetries - 1)
                {
                    return;
                }
                if (!bIsFileNotFound)
                {
                    Thread.Sleep(iBaseDelayMs * (iAttempt + 1));
                }
                TryWriteLog(iAttempt + 1);
            }
        }
        TryWriteLog(0);
    }
    private void CheckAndRotateLog()
    {
        Interlocked.Exchange(ref _LogEntriesSinceLastCheck, 0);
        long CurrentSize = Interlocked.Read(ref _CurrentLogFileSize);
        if (CurrentSize < _LogRotationThreshold)
            return;
        lock (_LogRotationLock)
        {
            CurrentSize = Interlocked.Read(ref _CurrentLogFileSize);
            if (CurrentSize < _LogRotationThreshold)
                return;

            try
            {
                string TimeStamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss");
                string BackupFileName = $"FileStorageLogs_{TimeStamp}.log";
                string BackupFilePath = Path.Combine(_StoragePath, BackupFileName);
                if (File.Exists(_LogFilePath))
                {
                    File.Move(_LogFilePath, BackupFilePath);
                }
                string NewLogHeader = $"Log File Created At UtcTime {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} LocalTime {DateTime.Now:yyyy-MM-dd HH:mm:ss}{Environment.NewLine}";
                File.WriteAllText(_LogFilePath, NewLogHeader, Encoding.UTF8);
                Interlocked.Exchange(ref _CurrentLogFileSize, Encoding.UTF8.GetByteCount(NewLogHeader));
                if (Interlocked.Increment(ref _RotationCount) % _BackupFileRotationCleanUp == 0)
                {
                    CleanupOldLogBackups();
                }
            }
            catch (Exception oEx)
            {
                try
                {
                    Debug.WriteLine($"Log rotation failed: {oEx.Message}");
                }
                catch { }
            }
        }
    }

    private void CleanupOldLogBackups()
    {
        try
        {
            var oBackupFiles = new DirectoryInfo(_StoragePath).EnumerateFiles("FileStorageLogs_*.log").OrderBy(f => f.CreationTimeUtc).ToList();
            while (oBackupFiles.Count > _MaxKeepBackupFile)
            {
                var oFileToDelete = oBackupFiles[0];
                try
                {
                    oFileToDelete.Delete();
                    oBackupFiles.RemoveAt(0);
                }
                catch
                {
                    oBackupFiles.RemoveAt(0);
                }
            }
        }
        catch { }
    }

    public void ForceCleanup()
    {
        OnCleanupTimer(null);
    }

    public void ForceCleanupOldFiles(int iHoursAgo = 24)
    {
        var dtCutoff = DateTime.UtcNow - TimeSpan.FromHours(iHoursAgo);
        var dtForceDeleteCutoff = DateTime.UtcNow - TimeSpan.FromHours(iHoursAgo) - TimeSpan.FromMinutes(_MaxActiveRefMinutes);
        LogMessage($"Forced cleanup. Cutoff time (UTC): {dtCutoff}");
        LogMessage($"Force delete cutoff (UTC): {dtForceDeleteCutoff}");

        var lstEntriesToDelete = new List<FileIndexEntry>();
        lock (_IndexLock)
        {
            foreach (var oEntry in _FileIndex.Values)
            {
                if (oEntry.CreateDate < dtForceDeleteCutoff)
                {
                    lstEntriesToDelete.Add(oEntry);
                }
                else if (oEntry.CreateDate < dtCutoff)
                {
                    if (Guid.TryParseExact(oEntry.FileId, "N", out Guid gFileId))
                    {
                        bool bHasActiveRef = _ActiveRefs.TryGetValue(gFileId, out int iCount) && iCount > 0;
                        if (!bHasActiveRef)
                        {
                            lstEntriesToDelete.Add(oEntry);
                        }
                    }
                }
            }
        }

        foreach (var oEntry in lstEntriesToDelete)
        {
            string sFilePath = Path.Combine(_StoragePath, oEntry.FileId + ".dat");
            if (DeleteFileAndRemoveFromIndex(sFilePath, oEntry))
            {
                LogMessage($"Force deleted: {oEntry.FileId}");
            }
            else
            {
                LogMessage($"Failed to force delete: {oEntry.FileId}");
            }
        }
    }

    public void RebuildIndexManually()
    {
        LogMessage("Manual index rebuild requested");
        RebuildIndex();
    }

    public void TestFileDeletion(string sFileId)
    {
        var dtCutoff = DateTime.UtcNow - TimeSpan.FromMinutes(_DeleteEveryMinutes);
        var dtForceDeleteCutoff = DateTime.UtcNow - TimeSpan.FromMinutes(_DeleteEveryMinutes + _MaxActiveRefMinutes);
        lock (_IndexLock)
        {
            if (_FileIndex.TryGetValue(sFileId, out var oEntry))
            {
                LogMessage($"Testing file: {sFileId}");
                LogMessage($"  File created: {oEntry.CreateDate}");
                LogMessage($"  Cutoff time: {dtCutoff}");
                LogMessage($"  Force delete cutoff: {dtForceDeleteCutoff}");
                LogMessage($"  Should delete: {oEntry.CreateDate < dtCutoff}");
                LogMessage($"  Should force delete: {oEntry.CreateDate < dtForceDeleteCutoff}");

                if (oEntry.CreateDate < dtForceDeleteCutoff)
                {
                    string sFilePath = Path.Combine(_StoragePath, oEntry.FileId + ".dat");
                    if (DeleteFileAndRemoveFromIndex(sFilePath, oEntry))
                    {
                        LogMessage($"  MANUALLY FORCE DELETED: {sFileId}");
                    }
                }
                else if (oEntry.CreateDate < dtCutoff)
                {
                    if (Guid.TryParseExact(oEntry.FileId, "N", out Guid gFileId))
                    {
                        bool bHasActiveRef = _ActiveRefs.TryGetValue(gFileId, out int iCount) && iCount > 0;
                        LogMessage($"  Has active reference: {bHasActiveRef}");
                        if (!bHasActiveRef)
                        {
                            string sFilePath = Path.Combine(_StoragePath, oEntry.FileId + ".dat");
                            if (DeleteFileAndRemoveFromIndex(sFilePath, oEntry))
                            {
                                LogMessage($"  MANUALLY DELETED: {sFileId}");
                            }
                        }
                    }
                }
            }
            else
            {
                LogMessage($"File not found in index: {sFileId}");
            }
        }
    }

    public void CleanupOrphanedReferences()
    {
        LogMessage("Cleaning up orphaned references");
        var lstOrphanedRefs = new List<Guid>();
        foreach (var kvp in _ActiveRefs)
        {
            string sFilePath = Path.Combine(_StoragePath, kvp.Key.ToString("N") + ".dat");
            if (!File.Exists(sFilePath))
            {
                lstOrphanedRefs.Add(kvp.Key);
            }
        }
        foreach (var gFileId in lstOrphanedRefs)
        {
            _ActiveRefs.TryRemove(gFileId, out _);
            LogMessage($"Removed orphaned reference: {gFileId}");
        }
        LogMessage($"Cleaned up {lstOrphanedRefs.Count} orphaned references");
    }

    public void Dispose()
    {
        if (Interlocked.CompareExchange(ref _Disposed, 1, 0) != 0)
            return;
        try
        {
            _oCleanupTimer.Dispose();
            if (_CleanupRunning != 0)
            {
                _CleanupCompleted.Wait(TimeSpan.FromSeconds(5));
            }
        }
        catch (Exception ex)
        {
            LogMessage($"Error during timer disposal: {ex}");
        }
        finally
        {
            _ActiveRefs.Clear();
            _CleanupCompleted.Dispose();
        }
    }
}