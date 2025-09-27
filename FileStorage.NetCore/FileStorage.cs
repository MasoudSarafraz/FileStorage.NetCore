using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
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
using System.Threading.Channels;

public sealed class FileStorage : IFileStorage
{
    private readonly string _StoragePath;
    private readonly uint _DeleteEveryMinutes;
    private readonly FileReferenceManager _ReferenceManager;
    private readonly FileIndexManager _IndexManager;
    private readonly FileCleanupManager _CleanupManager;
    private readonly LogManager _Logger;
    private readonly DiskSpaceChecker _DiskChecker;
    private PeriodicTimer _oCleanupTimer;
    private readonly ManualResetEventSlim _CleanupCompleted = new(true);
    private volatile int _CleanupRunning;
    private volatile int _Disposed;
    private static readonly Random _Random = Random.Shared;
    private const int _DefaultBufferSize = 8192;
    private const long _MaxFileSize = 100 * 1024 * 1024;
    private readonly uint _MaxActiveRefMinutes;

    private readonly record struct FileIndexEntry(string FileId, DateTime CreateDate);

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
            _oStorage._ReferenceManager.IncrementRef(_gFileId);
        }
        protected override void Dispose(bool bDisposing)
        {
            if (Interlocked.Exchange(ref _bDisposed, 1) == 0)
            {
                if (bDisposing)
                {
                    _oStorage._ReferenceManager.DecrementRef(_gFileId);
                }
                base.Dispose(bDisposing);
            }
        }
    }

    private class FileReferenceManager
    {
        private ImmutableDictionary<Guid, int> _ActiveRefs = ImmutableDictionary<Guid, int>.Empty;
        public void IncrementRef(Guid gFileId)
        {
            ImmutableInterlocked.Update(ref _ActiveRefs, dict =>
                dict.SetItem(gFileId, dict.TryGetValue(gFileId, out int count) ? count + 1 : 1));
        }
        public void DecrementRef(Guid gFileId)
        {
            ImmutableInterlocked.Update(ref _ActiveRefs, dict =>
            {
                if (dict.TryGetValue(gFileId, out int count) && count > 1)
                    return dict.SetItem(gFileId, count - 1);
                return dict.Remove(gFileId);
            });
        }
        public bool HasActiveRef(Guid gFileId)
        {
            return _ActiveRefs.TryGetValue(gFileId, out int iCount) && iCount > 0;
        }
        public ImmutableList<Guid> GetOrphanedRefs(string sStoragePath)
        {
            return _ActiveRefs
                .Where(kvp => !File.Exists(Path.Combine(sStoragePath, kvp.Key.ToString("N") + ".dat")))
                .Select(kvp => kvp.Key)
                .ToImmutableList();
        }
        public void RemoveOrphanedRefs(ImmutableList<Guid> lstOrphanedRefs)
        {
            ImmutableInterlocked.Update(ref _ActiveRefs, dict => dict.RemoveRange(lstOrphanedRefs));
        }
    }

    private class FileIndexManager
    {
        private readonly string _StoragePath;
        private readonly string _IndexFilePath;
        private readonly JsonSerializerOptions _JsonSettings;
        private readonly LogManager _Logger;
        private readonly int _MaxIndexEntries;
        private readonly int _IndexRebuildHours;
        private long _LastIndexRebuildTicks = DateTime.MinValue.ToBinary();
        private volatile int _isRebuilding = 0;
        private ImmutableDictionary<string, FileIndexEntry> _FileIndex = ImmutableDictionary<string, FileIndexEntry>.Empty;

        public FileIndexManager(string sStoragePath, LogManager oLogger)
        {
            _StoragePath = sStoragePath;
            _IndexFilePath = Path.Combine(sStoragePath, "FileIndex.json");
            _Logger = oLogger;
            _MaxIndexEntries = 10000;
            _IndexRebuildHours = 24;
            _JsonSettings = new JsonSerializerOptions
            {
                WriteIndented = true,
                Converters = { new JsonStringEnumConverter() },
                Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            };
        }

        public void LoadOrRebuildIndex()
        {
            if (File.Exists(_IndexFilePath))
            {
                try
                {
                    string sJson = File.ReadAllText(_IndexFilePath);
                    var loadedIndex = JsonSerializer.Deserialize<List<FileIndexEntry>>(sJson, _JsonSettings) ?? new List<FileIndexEntry>();
                    _FileIndex = loadedIndex.ToImmutableDictionary(e => e.FileId);
                    if (_FileIndex.Count > _MaxIndexEntries ||
                        (DateTime.UtcNow - LastRebuildTime) > TimeSpan.FromHours(_IndexRebuildHours))
                    {
                        _Logger.LogMessage($"Index needs rebuild. Entries: {_FileIndex.Count}, Last rebuild: {LastRebuildTime}");
                        RebuildIndex();
                    }
                    else
                    {
                        _Logger.LogMessage($"Index loaded successfully. Entries: {_FileIndex.Count}");
                    }
                }
                catch (Exception ex)
                {
                    _Logger.LogMessage($"Failed to load index: {ex}. Rebuilding...");
                    RebuildIndex();
                }
            }
            else
            {
                _Logger.LogMessage("Index file not found. Rebuilding from existing files");
                RebuildIndex();
            }
        }

        public void RebuildIndex()
        {
            if (Interlocked.CompareExchange(ref _isRebuilding, 1, 0) != 0) return;
            try
            {
                var newIndexBuilder = ImmutableDictionary.CreateBuilder<string, FileIndexEntry>();
                var oDirInfo = new DirectoryInfo(_StoragePath);
                foreach (var oFileInfo in oDirInfo.EnumerateFiles("*.dat"))
                {
                    string sFileName = Path.GetFileNameWithoutExtension(oFileInfo.Name);
                    if (Guid.TryParseExact(sFileName, "N", out Guid gGuid))
                    {
                        DateTime dtCreated = GetFileCreationTime(oFileInfo);
                        newIndexBuilder[gGuid.ToString("N")] = new FileIndexEntry(gGuid.ToString("N"), dtCreated);
                    }
                }
                ImmutableInterlocked.Update(ref _FileIndex, _ => newIndexBuilder.ToImmutable());
                SaveIndex();
                LastRebuildTime = DateTime.UtcNow;
                _Logger.LogMessage($"Index rebuilt successfully. Entries: {_FileIndex.Count}");
            }
            catch (Exception ex)
            {
                _Logger.LogMessage($"Failed to rebuild index: {ex}");
            }
            finally
            {
                Interlocked.Exchange(ref _isRebuilding, 0);
            }
        }

        private DateTime GetFileCreationTime(FileInfo oFileInfo)
        {
            try
            {
                return oFileInfo.CreationTimeUtc;
            }
            catch
            {
                try
                {
                    return oFileInfo.CreationTime.ToUniversalTime();
                }
                catch
                {
                    try
                    {
                        return oFileInfo.LastWriteTimeUtc;
                    }
                    catch
                    {
                        return DateTime.UtcNow;
                    }
                }
            }
        }

        public void SaveIndex()
        {
            const int iMaxRetries = 3;
            const int iBaseDelayMs = 100;
            string sTempPath = Path.Combine(_StoragePath, $"FileIndex_{Guid.NewGuid():N}.tmp");
            for (int iAttempt = 1; iAttempt <= iMaxRetries; iAttempt++)
            {
                try
                {
                    if (File.Exists(sTempPath)) File.Delete(sTempPath);
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
                    _Logger.LogMessage($"Attempt {iAttempt} to save index failed: {ex.Message}. Retrying...");
                    Thread.Sleep(iBaseDelayMs * iAttempt);
                }
                catch (Exception ex)
                {
                    _Logger.LogMessage($"Failed to save index after {iMaxRetries} attempts: {ex}");
                    try
                    {
                        string sBackupPath = Path.Combine(_StoragePath, "FileIndex_backup.json");
                        string sJson = JsonSerializer.Serialize(_FileIndex.Values.ToList(), _JsonSettings);
                        File.WriteAllText(sBackupPath, sJson, Encoding.UTF8);
                        if (File.Exists(_IndexFilePath)) File.Delete(_IndexFilePath);
                        File.Move(sBackupPath, _IndexFilePath);
                        _Logger.LogMessage("Index saved using fallback method");
                        return;
                    }
                    catch (Exception fallbackEx)
                    {
                        _Logger.LogMessage($"Fallback save also failed: {fallbackEx}");
                    }
                }
                finally
                {
                    if (File.Exists(sTempPath)) File.Delete(sTempPath);
                }
            }
        }

        public void AddToIndex(Guid gFileId, DateTime dtCreated)
        {
            dtCreated = dtCreated.ToUniversalTime();
            var newEntry = new FileIndexEntry(gFileId.ToString("N"), dtCreated);
            ImmutableInterlocked.Update(ref _FileIndex, dict => dict.Add(newEntry.FileId, newEntry));
            SaveIndex();
        }

        public void RemoveFromIndex(Guid gFileId)
        {
            ImmutableInterlocked.Update(ref _FileIndex, dict => dict.Remove(gFileId.ToString("N")));
            SaveIndex();
        }

        public ImmutableList<FileIndexEntry> GetEntriesToDelete(DateTime dtCutoff, DateTime dtForceDeleteCutoff)
        {
            return _FileIndex.Values
                .Where(entry => entry.CreateDate < dtForceDeleteCutoff ||
                               (entry.CreateDate < dtCutoff && Guid.TryParseExact(entry.FileId, "N", out _)))
                .ToImmutableList();
        }

        public bool TryGetEntry(string sFileId, out FileIndexEntry oEntry)
        {
            return _FileIndex.TryGetValue(sFileId, out oEntry);
        }

        public bool ContainsKey(string sFileId)
        {
            return _FileIndex.ContainsKey(sFileId);
        }

        public void RemoveEntry(string sFileId)
        {
            ImmutableInterlocked.Update(ref _FileIndex, dict => dict.Remove(sFileId));
            SaveIndex();
        }

        public int Count => _FileIndex.Count;
        public DateTime LastRebuildTime
        {
            get => DateTime.FromBinary(Interlocked.Read(ref _LastIndexRebuildTicks));
            set => Interlocked.Exchange(ref _LastIndexRebuildTicks, value.ToBinary());
        }
        public IImmutableDictionary<string, FileIndexEntry> GetAllEntries() => _FileIndex;
    }

    private class FileCleanupManager
    {
        private readonly string _StoragePath;
        private readonly FileIndexManager _IndexManager;
        private readonly FileReferenceManager _ReferenceManager;
        private readonly LogManager _Logger;
        private readonly uint _DeleteEveryMinutes;
        private readonly uint _MaxActiveRefMinutes;

        public FileCleanupManager(string sStoragePath, FileIndexManager oIndexManager, FileReferenceManager oReferenceManager, LogManager oLogger, uint iDeleteEveryMinutes, uint iMaxActiveRefMinutes)
        {
            _StoragePath = sStoragePath;
            _IndexManager = oIndexManager;
            _ReferenceManager = oReferenceManager;
            _Logger = oLogger;
            _DeleteEveryMinutes = iDeleteEveryMinutes;
            _MaxActiveRefMinutes = iMaxActiveRefMinutes;
        }

        public void CleanupOldFiles()
        {
            try
            {
                var dtCutoff = DateTime.UtcNow - TimeSpan.FromMinutes(_DeleteEveryMinutes);
                var dtForceDeleteCutoff = DateTime.UtcNow - TimeSpan.FromMinutes(_DeleteEveryMinutes + _MaxActiveRefMinutes);
                _Logger.LogMessage($"Cleanup started. Cutoff: {dtCutoff}, Force: {dtForceDeleteCutoff}");

                RemoveStaleEntries();

                if ((DateTime.UtcNow - _IndexManager.LastRebuildTime) > TimeSpan.FromHours(24))
                {
                    _Logger.LogMessage("Scheduled index rebuild");
                    _IndexManager.RebuildIndex();
                }

                int iDeletedCount = 0;
                int iFailedCount = 0;

                var lstEntriesToDelete = _IndexManager.GetEntriesToDelete(dtCutoff, dtForceDeleteCutoff);
                _Logger.LogMessage($"Found {lstEntriesToDelete.Count} files to delete from index");

                foreach (var oEntry in lstEntriesToDelete)
                {
                    string sFilePath = Path.Combine(_StoragePath, oEntry.FileId + ".dat");
                    if (DeleteFileAndRemoveFromIndex(sFilePath, oEntry))
                    {
                        iDeletedCount++;
                        _Logger.LogMessage($"SUCCESSFULLY deleted: {oEntry.FileId}");
                    }
                    else
                    {
                        iFailedCount++;
                        _Logger.LogMessage($"FAILED to delete: {oEntry.FileId}");
                    }
                }

                var oDirInfo = new DirectoryInfo(_StoragePath);
                var allFiles = oDirInfo.GetFiles("*.dat");
                _Logger.LogMessage($"Found {allFiles.Length} .dat files in directory");

                foreach (var oFileInfo in allFiles)
                {
                    string sFileName = Path.GetFileNameWithoutExtension(oFileInfo.Name);
                    if (Guid.TryParseExact(sFileName, "N", out Guid gFileId))
                    {
                        if (!_IndexManager.ContainsKey(gFileId.ToString("N")))
                        {
                            DateTime dtCreated = GetFileCreationTime(oFileInfo);
                            if (dtCreated < dtForceDeleteCutoff || dtCreated < dtCutoff)
                            {
                                if (SafeDeleteFile(oFileInfo.FullName))
                                {
                                    iDeletedCount++;
                                    _Logger.LogMessage($"Deleted old file not in index: {sFileName}");
                                }
                                else
                                {
                                    iFailedCount++;
                                    _Logger.LogMessage($"Failed to delete file not in index: {sFileName}");
                                }
                            }
                        }
                    }
                }

                foreach (var oFileInfo in oDirInfo.EnumerateFiles("*.tmp"))
                {
                    if (oFileInfo.Name.Equals("FileIndex.tmp", StringComparison.OrdinalIgnoreCase)) continue;
                    if (SafeDeleteFile(oFileInfo.FullName))
                    {
                        iDeletedCount++;
                        _Logger.LogMessage($"Deleted temp file: {oFileInfo.Name}");
                    }
                    else
                    {
                        iFailedCount++;
                    }
                }

                _Logger.LogMessage($"Cleanup completed. Deleted: {iDeletedCount}, Failed: {iFailedCount}");
            }
            catch (Exception ex)
            {
                _Logger.LogMessage($"Global cleanup error: {ex}");
            }
        }

        private DateTime GetFileCreationTime(FileInfo oFileInfo)
        {
            try
            {
                return oFileInfo.CreationTimeUtc;
            }
            catch
            {
                try
                {
                    return oFileInfo.CreationTime.ToUniversalTime();
                }
                catch
                {
                    try
                    {
                        return oFileInfo.LastWriteTimeUtc;
                    }
                    catch
                    {
                        return DateTime.UtcNow;
                    }
                }
            }
        }

        private bool DeleteFileAndRemoveFromIndex(string sFilePath, FileIndexEntry oEntry)
        {
            if (SafeDeleteFile(sFilePath))
            {
                _IndexManager.RemoveEntry(oEntry.FileId);
                return true;
            }
            return false;
        }

        private bool SafeDeleteFile(string sPath)
        {
            const int iMaxRetries = 3;
            const int iBaseDelayMs = 100;
            for (int i = 0; i < iMaxRetries; i++)
            {
                try
                {
                    if (!File.Exists(sPath)) return true;
                    var oFileInfo = new FileInfo(sPath);
                    if (oFileInfo.IsReadOnly)
                    {
                        oFileInfo.IsReadOnly = false;
                        _Logger.LogMessage($"Removed read-only attribute from file: {sPath}");
                    }
                    File.Delete(sPath);
                    _Logger.LogMessage($"Successfully deleted file: {sPath}");
                    return true;
                }
                catch (Exception ex) when (ex is FileNotFoundException || ex is DirectoryNotFoundException)
                {
                    return true;
                }
                catch (Exception ex) when (i < iMaxRetries - 1 && (ex is IOException || ex is UnauthorizedAccessException))
                {
                    _Logger.LogMessage($"Attempt {i + 1} to delete file '{sPath}' failed: {ex.Message}. Retrying...");
                    Thread.Sleep(iBaseDelayMs * (i + 1));
                }
                catch (Exception ex)
                {
                    _Logger.LogMessage($"Critical error deleting file '{sPath}': {ex}");
                    return false;
                }
            }
            _Logger.LogMessage($"Failed to delete file '{sPath}' after {iMaxRetries} attempts");
            return false;
        }

        private void RemoveStaleEntries()
        {
            var staleEntries = _IndexManager.GetAllEntries()
                .Where(entry => !File.Exists(Path.Combine(_StoragePath, entry.Key + ".dat")))
                .Select(entry => entry.Key)
                .ToImmutableList();

            foreach (var sFileId in staleEntries)
            {
                _IndexManager.RemoveEntry(sFileId);
            }
            if (staleEntries.Count > 0)
            {
                _Logger.LogMessage($"Removed {staleEntries.Count} stale entries from index");
            }
        }

        public void CleanupTempIndexFiles()
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
                                _Logger.LogMessage($"Deleted old temp file: {oFile.Name}");
                            }
                        }
                        catch (Exception ex)
                        {
                            _Logger.LogMessage($"Failed to delete temp file {oFile.Name}: {ex}");
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
                            _Logger.LogMessage($"Deleted old random temp file: {oFile.Name}");
                        }
                    }
                    catch (Exception ex)
                    {
                        _Logger.LogMessage($"Failed to delete random temp file {oFile.Name}: {ex}");
                    }
                }
            }
            catch (Exception ex)
            {
                _Logger.LogMessage($"Failed to cleanup temp files: {ex}");
            }
        }

        public void CleanupOrphanedReferences()
        {
            _Logger.LogMessage("Cleaning up orphaned references");
            var lstOrphanedRefs = _ReferenceManager.GetOrphanedRefs(_StoragePath);
            _ReferenceManager.RemoveOrphanedRefs(lstOrphanedRefs);
            _Logger.LogMessage($"Cleaned up {lstOrphanedRefs.Count} orphaned references");
        }
    }

    private class LogManager
    {
        private readonly string _LogFilePath;
        private readonly string _StoragePath;
        private readonly Channel<string> _logChannel;
        private readonly Task _logWriterTask;
        private readonly CancellationTokenSource _logCts = new();
        private long _CurrentLogFileSize = 0;
        private int _LogEntriesSinceLastCheck = 0;
        private const long _LogRotationThreshold = 5 * 1024 * 1024;
        private const int _LogCheckInterval = 100;
        private int _RotationCount = 0;
        private readonly int _MaxKeepBackupFile = 5;
        private readonly int _BackupFileRotationCleanUp = 10;

        public LogManager(string sStoragePath)
        {
            _StoragePath = sStoragePath;
            _LogFilePath = Path.Combine(sStoragePath, "FileStorageLogs.log");
            _logChannel = Channel.CreateUnbounded<string>(new UnboundedChannelOptions { SingleReader = true });

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
            _logWriterTask = Task.Run(() => LogWriterLoop(_logCts.Token));
        }

        private async Task LogWriterLoop(CancellationToken ct)
        {
            while (await _logChannel.Reader.WaitToReadAsync(ct))
            {
                while (_logChannel.Reader.TryRead(out string sLogEntry))
                {
                    WriteLogEntry(sLogEntry);
                }
            }
        }

        private void WriteLogEntry(string sLogEntry)
        {
            int EntrySize = Encoding.UTF8.GetByteCount(sLogEntry);
            if (Interlocked.Increment(ref _LogEntriesSinceLastCheck) >= _LogCheckInterval)
            {
                CheckAndRotateLog();
            }
            const int MaxRetries = 2;
            const int iBaseDelayMs = 100;
            for (int iAttempt = 0; iAttempt < MaxRetries; iAttempt++)
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
                    if (iAttempt == MaxRetries - 1) return;
                    Thread.Sleep(iBaseDelayMs * (iAttempt + 1));
                }
            }
        }

        public void LogMessage(string sMessage)
        {
            string sLogEntry = $"UtcTime:{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss}=>LocalTime:{DateTime.Now:yyyy-MM-dd HH:mm:ss}: {sMessage}{Environment.NewLine}";
            _logChannel.Writer.TryWrite(sLogEntry);
        }

        private void CheckAndRotateLog()
        {
            Interlocked.Exchange(ref _LogEntriesSinceLastCheck, 0);
            long CurrentSize = Interlocked.Read(ref _CurrentLogFileSize);
            if (CurrentSize < _LogRotationThreshold) return;
            try
            {
                string TimeStamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss");
                string BackupFileName = $"FileStorageLogs_{TimeStamp}.log";
                string BackupFilePath = Path.Combine(_StoragePath, BackupFileName);
                if (File.Exists(_LogFilePath)) File.Move(_LogFilePath, BackupFilePath);
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
                try { Debug.WriteLine($"Log rotation failed: {oEx.Message}"); } catch { }
            }
        }

        private void CleanupOldLogBackups()
        {
            try
            {
                var oBackupFiles = new DirectoryInfo(_StoragePath)
                    .EnumerateFiles("FileStorageLogs_*.log")
                    .OrderBy(f => f.CreationTimeUtc)
                    .ToImmutableList();

                foreach (var oFileToDelete in oBackupFiles.Take(oBackupFiles.Count - _MaxKeepBackupFile))
                {
                    try { oFileToDelete.Delete(); } catch { }
                }
            }
            catch { }
        }

        public void Dispose()
        {
            _logCts.Cancel();
            _logChannel.Writer.Complete();
            try { _logWriterTask.Wait(TimeSpan.FromSeconds(2)); } catch { }
        }
    }

    private class DiskSpaceChecker
    {
        private readonly DriveInfo _DriveInfo;
        private readonly double _FreeSpaceBufferRatio = 0.1;
        private long _LastDiskCheckTime;
        private long _LastFreeSpace;

        public DiskSpaceChecker(string sStoragePath)
        {
            _DriveInfo = new DriveInfo(Path.GetPathRoot(sStoragePath));
        }

        public void EnsureDiskSpace(long lRequiredSpace)
        {
            long lNow = Stopwatch.GetTimestamp();
            long lastCheckTime = Interlocked.Read(ref _LastDiskCheckTime);
            long lastFreeSpace = Interlocked.Read(ref _LastFreeSpace);
            if (lNow - lastCheckTime < TimeSpan.TicksPerSecond * 5)
            {
                if (lastFreeSpace >= lRequiredSpace * (1 + _FreeSpaceBufferRatio)) return;
            }
            try
            {
                long lCurrentFreeSpace = _DriveInfo.AvailableFreeSpace;
                Interlocked.Exchange(ref _LastFreeSpace, lCurrentFreeSpace);
                Interlocked.Exchange(ref _LastDiskCheckTime, lNow);
                long lRequiredWithBuffer = (long)(lRequiredSpace * (1 + _FreeSpaceBufferRatio));
                if (lCurrentFreeSpace < lRequiredWithBuffer) throw new IOException("Not enough disk space available");
            }
            catch (Exception ex)
            {
                throw new IOException("Failed to check disk space", ex);
            }
        }
    }

    public FileStorage(string sPath, uint iDeleteEveryMinute = 60)
    {
        if (string.IsNullOrWhiteSpace(sPath)) throw new ArgumentNullException(nameof(sPath));
        try
        {
            _StoragePath = Path.GetFullPath(sPath);
            if (!Path.IsPathRooted(_StoragePath)) throw new ArgumentException("Path must be rooted", nameof(sPath));
        }
        catch (Exception ex) when (ex is ArgumentException || ex is PathTooLongException || ex is NotSupportedException)
        {
            throw new ArgumentException("Invalid storage path", nameof(sPath), ex);
        }
        Directory.CreateDirectory(_StoragePath);
        _Logger = new LogManager(_StoragePath);
        _IndexManager = new FileIndexManager(_StoragePath, _Logger);
        _ReferenceManager = new FileReferenceManager();
        _CleanupManager = new FileCleanupManager(_StoragePath, _IndexManager, _ReferenceManager, _Logger, iDeleteEveryMinute, (uint)(iDeleteEveryMinute * 2));
        _DiskChecker = new DiskSpaceChecker(_StoragePath);
        _DeleteEveryMinutes = iDeleteEveryMinute;
        _MaxActiveRefMinutes = (uint)(iDeleteEveryMinute * 2);
        CheckPermissions();
        _CleanupManager.CleanupTempIndexFiles();
        _IndexManager.LoadOrRebuildIndex();
        var oDelay = TimeSpan.FromMinutes(iDeleteEveryMinute);
        _oCleanupTimer = new PeriodicTimer(TimeSpan.FromMilliseconds(30000));
        _ = RunCleanupTimerAsync(oDelay);
        _Logger.LogMessage($"FileStorage initialized. Path: {_StoragePath}, Cleanup interval: {iDeleteEveryMinute} minute");
    }

    private async Task RunCleanupTimerAsync(TimeSpan oInterval)
    {
        while (await _oCleanupTimer.WaitForNextTickAsync())
        {
            OnCleanupTimer(null);
            _oCleanupTimer.Period = oInterval;
        }
    }

    private void CheckPermissions()
    {
        try
        {
            string sTestFile = Path.Combine(_StoragePath, "permission_test.tmp");
            File.WriteAllText(sTestFile, "test");
            File.Delete(sTestFile);
            _Logger.LogMessage("Permission check passed successfully");
        }
        catch (Exception ex)
        {
            throw new Exception($"The application does not have write/delete permissions on the storage path: {_StoragePath} Please grant 'Modify' permissions. Error: {ex.Message}", ex);
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
            return new ReferenceCountedFileStream(this, gFileId, sFilePath, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete, _DefaultBufferSize, FileOptions.RandomAccess | FileOptions.SequentialScan);
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
            return new ReferenceCountedFileStream(this, gFileId, sFilePath, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete, _DefaultBufferSize, FileOptions.Asynchronous | FileOptions.SequentialScan);
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
        if (lFileSize > _MaxFileSize) throw new IOException($"File is too large to load into memory (max {_MaxFileSize} bytes)");
        _ReferenceManager.IncrementRef(gFileId);
        try
        {
            return ExecuteWithRetry(() =>
            {
                int iBufferSize = GetOptimizedBufferSize(lFileSize);
                using (var oStream = new FileStream(sFilePath, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete, iBufferSize, FileOptions.SequentialScan))
                {
                    return ReadAllBytes(oStream, lFileSize);
                }
            });
        }
        finally
        {
            _ReferenceManager.DecrementRef(gFileId);
        }
    }

    public async Task<byte[]> GetFileBytesAsync(Guid gFileId, CancellationToken oCt = default)
    {
        CheckDisposed();
        var sFilePath = GetFilePath(gFileId);
        var lFileSize = new FileInfo(sFilePath).Length;
        if (lFileSize > _MaxFileSize) throw new IOException($"File is too large to load into memory (max {_MaxFileSize} bytes)");
        _ReferenceManager.IncrementRef(gFileId);
        try
        {
            return await ExecuteWithRetryAsync(async () =>
            {
                int iBufferSize = GetOptimizedBufferSize(lFileSize);
                using (var oStream = new FileStream(sFilePath, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete, iBufferSize, FileOptions.Asynchronous | FileOptions.SequentialScan))
                {
                    return await ReadAllBytesAsync(oStream, lFileSize, oCt);
                }
            }, oCt);
        }
        finally
        {
            _ReferenceManager.DecrementRef(gFileId);
        }
    }

    private byte[] ReadAllBytes(FileStream stream, long fileSize)
    {
        var buffer = new byte[fileSize];
        int bytesRead = 0;
        while (bytesRead < fileSize)
        {
            int read = stream.Read(buffer, bytesRead, (int)(fileSize - bytesRead));
            if (read == 0) throw new IOException("File read incomplete");
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
            if (read == 0) throw new IOException("File read incomplete");
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
        if (oStream == null) throw new ArgumentNullException(nameof(oStream));
        long lFileSize = lKnownLength ?? (oStream.CanSeek ? oStream.Length : _MaxFileSize);
        if (lFileSize > _MaxFileSize) throw new IOException($"File size exceeds maximum allowed size of {_MaxFileSize} bytes");
        _DiskChecker.EnsureDiskSpace(lFileSize);
        var gFileId = Guid.NewGuid();
        var sTempPath = GetTempPath(gFileId);
        var sFinalPath = GetFilePath(gFileId);
        int iBufferSize = oStream.CanSeek && lKnownLength.HasValue ? GetOptimizedBufferSize(lKnownLength.Value) : _DefaultBufferSize;
        _ReferenceManager.IncrementRef(gFileId);
        try
        {
            ExecuteWithRetry(() =>
            {
                using (var oFs = new FileStream(sTempPath, FileMode.CreateNew, FileAccess.Write, FileShare.None, iBufferSize, FileOptions.SequentialScan))
                {
                    oStream.CopyTo(oFs, iBufferSize);
                }
                File.Move(sTempPath, sFinalPath);
                _IndexManager.AddToIndex(gFileId, DateTime.UtcNow);
            });
        }
        catch
        {
            SafeDeleteFile(sTempPath);
            throw;
        }
        finally
        {
            _ReferenceManager.DecrementRef(gFileId);
        }
        return gFileId;
    }

    private async Task<Guid> SaveFileCoreAsync(Stream oStream, long? lKnownLength, CancellationToken oCt = default)
    {
        CheckDisposed();
        if (oStream == null) throw new ArgumentNullException(nameof(oStream));
        long lFileSize = lKnownLength ?? (oStream.CanSeek ? oStream.Length : _MaxFileSize);
        if (lFileSize > _MaxFileSize) throw new IOException($"File size exceeds maximum allowed size of {_MaxFileSize} bytes");
        _DiskChecker.EnsureDiskSpace(lFileSize);
        var gFileId = Guid.NewGuid();
        var sTempPath = GetTempPath(gFileId);
        var sFinalPath = GetFilePath(gFileId);
        int iBufferSize = oStream.CanSeek && lKnownLength.HasValue ? GetOptimizedBufferSize(lKnownLength.Value) : _DefaultBufferSize;
        _ReferenceManager.IncrementRef(gFileId);
        try
        {
            await ExecuteWithRetryAsync(async () =>
            {
                await using (var oFs = new FileStream(sTempPath, FileMode.CreateNew, FileAccess.Write, FileShare.None, iBufferSize, FileOptions.Asynchronous | FileOptions.SequentialScan))
                {
                    await oStream.CopyToAsync(oFs, iBufferSize, oCt).ConfigureAwait(false);
                }
                File.Move(sTempPath, sFinalPath);
                _IndexManager.AddToIndex(gFileId, DateTime.UtcNow);
            }, oCt).ConfigureAwait(false);
        }
        catch
        {
            SafeDeleteFile(sTempPath);
            throw;
        }
        finally
        {
            _ReferenceManager.DecrementRef(gFileId);
        }
        return gFileId;
    }

    private void OnCleanupTimer(object oState)
    {
        if (_Disposed != 0) return;
        if (Interlocked.CompareExchange(ref _CleanupRunning, 1, 0) != 0) return;
        _CleanupCompleted.Reset();
        try
        {
            _Logger.LogMessage("Cleanup timer triggered");
            _CleanupManager.CleanupTempIndexFiles();
            _CleanupManager.CleanupOrphanedReferences();
            _CleanupManager.CleanupOldFiles();
        }
        catch (Exception ex)
        {
            _Logger.LogMessage($"Cleanup failed: {ex}");
        }
        finally
        {
            Interlocked.Exchange(ref _CleanupRunning, 0);
            _CleanupCompleted.Set();
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
                if (!File.Exists(sPath)) return true;
                var oFileInfo = new FileInfo(sPath);
                if (oFileInfo.IsReadOnly)
                {
                    oFileInfo.IsReadOnly = false;
                    _Logger.LogMessage($"Removed read-only attribute from file: {sPath}");
                }
                File.Delete(sPath);
                _Logger.LogMessage($"Successfully deleted file: {sPath}");
                return true;
            }
            catch (Exception ex) when (ex is FileNotFoundException || ex is DirectoryNotFoundException)
            {
                return true;
            }
            catch (Exception ex) when (i < iMaxRetries - 1 && (ex is IOException || ex is UnauthorizedAccessException))
            {
                _Logger.LogMessage($"Attempt {i + 1} to delete file '{sPath}' failed: {ex.Message}. Retrying...");
                Thread.Sleep(iBaseDelayMs * (i + 1));
            }
            catch (Exception ex)
            {
                _Logger.LogMessage($"Critical error deleting file '{sPath}': {ex}");
                return false;
            }
        }
        _Logger.LogMessage($"Failed to delete file '{sPath}' after {iMaxRetries} attempts");
        return false;
    }

    private string GetFilePath(Guid gFileId) => Path.Combine(_StoragePath, gFileId.ToString("N") + ".dat");
    private string GetTempPath(Guid gFileId) => Path.Combine(_StoragePath, gFileId.ToString("N") + ".tmp");

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
        ExecuteWithRetry(() => { oAction(); return true; }, iMaxRetries, iBaseDelay);
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
        await ExecuteWithRetryAsync(async () => { await oAction().ConfigureAwait(false); return true; }, oCt, iMaxRetries, iBaseDelay).ConfigureAwait(false);
    }

    private void CheckDisposed()
    {
        if (_Disposed != 0) throw new ObjectDisposedException("FileStorage");
    }

    private bool IsTransientFileError(Exception oEx)
    {
        return oEx is IOException || oEx is UnauthorizedAccessException;
    }

    public void ForceCleanup()
    {
        OnCleanupTimer(null);
    }

    public void ForceCleanupOldFiles(int iHoursAgo = 24)
    {
        var dtCutoff = DateTime.UtcNow - TimeSpan.FromHours(iHoursAgo);
        var dtForceDeleteCutoff = DateTime.UtcNow - TimeSpan.FromHours(iHoursAgo) - TimeSpan.FromMinutes(_MaxActiveRefMinutes);
        _Logger.LogMessage($"Forced cleanup. Cutoff: {dtCutoff}, Force: {dtForceDeleteCutoff}");

        var lstEntriesToDelete = _IndexManager.GetEntriesToDelete(dtCutoff, dtForceDeleteCutoff);
        foreach (var oEntry in lstEntriesToDelete)
        {
            string sFilePath = Path.Combine(_StoragePath, oEntry.FileId + ".dat");
            if (SafeDeleteFile(sFilePath))
            {
                _IndexManager.RemoveEntry(oEntry.FileId);
                _Logger.LogMessage($"Force deleted: {oEntry.FileId}");
            }
            else
            {
                _Logger.LogMessage($"Failed to force delete: {oEntry.FileId}");
            }
        }
    }

    public void RebuildIndexManually()
    {
        _Logger.LogMessage("Manual index rebuild requested");
        _IndexManager.RebuildIndex();
    }

    public void TestFileDeletion(string sFileId)
    {
        var dtCutoff = DateTime.UtcNow - TimeSpan.FromMinutes(_DeleteEveryMinutes);
        var dtForceDeleteCutoff = DateTime.UtcNow - TimeSpan.FromMinutes(_DeleteEveryMinutes + _MaxActiveRefMinutes);
        if (_IndexManager.TryGetEntry(sFileId, out var oEntry))
        {
            _Logger.LogMessage($"Testing file: {sFileId}, Created: {oEntry.CreateDate}, Cutoff: {dtCutoff}, Force: {dtForceDeleteCutoff}");
            if (oEntry.CreateDate < dtForceDeleteCutoff)
            {
                string sFilePath = Path.Combine(_StoragePath, oEntry.FileId + ".dat");
                if (SafeDeleteFile(sFilePath))
                {
                    _IndexManager.RemoveEntry(oEntry.FileId);
                    _Logger.LogMessage($"MANUALLY FORCE DELETED: {sFileId}");
                }
            }
            else if (oEntry.CreateDate < dtCutoff)
            {
                if (Guid.TryParseExact(oEntry.FileId, "N", out Guid gFileId))
                {
                    if (!_ReferenceManager.HasActiveRef(gFileId))
                    {
                        string sFilePath = Path.Combine(_StoragePath, oEntry.FileId + ".dat");
                        if (SafeDeleteFile(sFilePath))
                        {
                            _IndexManager.RemoveEntry(oEntry.FileId);
                            _Logger.LogMessage($"MANUALLY DELETED: {sFileId}");
                        }
                    }
                }
            }
        }
        else
        {
            _Logger.LogMessage($"File not found in index: {sFileId}");
        }
    }

    public void CleanupOrphanedReferences()
    {
        _CleanupManager.CleanupOrphanedReferences();
    }

    public void Dispose()
    {
        if (Interlocked.CompareExchange(ref _Disposed, 1, 0) != 0) return;
        try
        {
            _oCleanupTimer.Dispose();
            _Logger.Dispose();
            if (_CleanupRunning != 0) _CleanupCompleted.Wait(TimeSpan.FromSeconds(5));
        }
        catch (Exception ex)
        {
            _Logger.LogMessage($"Error during disposal: {ex}");
        }
        finally
        {
            _CleanupCompleted.Dispose();
        }
    }
}