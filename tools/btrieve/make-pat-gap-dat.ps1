# Has a real Btrieve 6.x engine build the test files assets/PATGAP1536.DAT and
# assets/PATGAP512.DAT: v6 files grown past 32 PAT pairs, so they have the
# 4096 bytes Btrieve leaves ahead of PAT pair 31 (see kGapBeforePAT31 in
# btrieve/BtrieveDatabase.cc). With 1536-byte pages that gap moves every page
# after it off the page grid; with 512-byte pages it is 8 whole pages.
#
#   powershell -ExecutionPolicy Bypass -File make-pat-gap-dat.ps1 OUTDIR ENGINEDIR
#
# ENGINEDIR holds WBTRV32.DLL and the workstation engine W32MKDE.EXE (Btrieve
# 6.15 for Windows NT/95; Nightmare Redux ships both). The engine is 32-bit, so
# this re-runs itself under the 32-bit Windows PowerShell when started from a
# 64-bit one.
#
# Each file has 250-byte records with a 4-byte integer key, the record's id
# from 1 to its count, and then "record NNNNNNNN " repeated. Once all are
# inserted, every 5th is updated to read "UPDATED NNNNNNNN " instead, which
# makes Btrieve write new copies of those data pages, and then every 7th is
# deleted. BtrieveDatabase_test.cc expects exactly those records back.
param(
    [Parameter(Mandatory = $true)][string]$Out,
    [Parameter(Mandatory = $true)][string]$Engine
)
$ErrorActionPreference = 'Stop'

if ([IntPtr]::Size -ne 4) {
    $ps32 = Join-Path $env:SystemRoot 'SysWOW64\WindowsPowerShell\v1.0\powershell.exe'
    & $ps32 -NoProfile -NonInteractive -ExecutionPolicy Bypass -File $PSCommandPath $Out $Engine
    exit $LASTEXITCODE
}

$Engine = (Resolve-Path $Engine).Path
New-Item -ItemType Directory -Force -Path $Out | Out-Null
$Out = (Resolve-Path $Out).Path
$env:PATH = "$Engine;$env:PATH"
Set-Location $Engine

Add-Type -TypeDefinition @'
using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;

public static class PatGapDat {
    [DllImport("WBTRV32.DLL", CallingConvention = CallingConvention.StdCall)]
    static extern short BTRCALL(ushort op, byte[] pos, byte[] data, ref int len, byte[] key, short keyLen, short keyNum);

    const ushort OPEN = 0, CLOSE = 1, INSERT = 2, UPDATE = 3, DELETE = 4, GET_EQUAL = 5,
        CREATE = 14, STOP = 25;
    public const int RecordLength = 250;

    static void Check(short status, string what) {
        if (status != 0) throw new Exception(what + ": Btrieve status " + status);
    }

    static byte[] Name(string file) {
        var key = new byte[255];
        var name = Encoding.ASCII.GetBytes(file + "\0");
        Array.Copy(name, key, name.Length);
        return key;
    }

    static byte[] Key(int id) {
        var key = new byte[255];
        BitConverter.GetBytes(id).CopyTo(key, 0);
        return key;
    }

    // Record id: the id as a little-endian int, then "record NNNNNNNN " (or
    // "UPDATED NNNNNNNN ") repeated to fill the record.
    public static byte[] Record(int id, bool updated) {
        var record = new byte[RecordLength];
        BitConverter.GetBytes(id).CopyTo(record, 0);
        var text = Encoding.ASCII.GetBytes((updated ? "UPDATED " : "record ") + id.ToString("D8") + " ");
        for (int at = 4; at < RecordLength; at++) {
            record[at] = text[(at - 4) % text.Length];
        }
        return record;
    }

    public static string Make(string path, int pageSize, int count) {
        // the file spec (16 bytes) and the key's spec (16 bytes)
        var spec = new byte[32];
        BitConverter.GetBytes((ushort)RecordLength).CopyTo(spec, 0);
        BitConverter.GetBytes((ushort)pageSize).CopyTo(spec, 2);
        BitConverter.GetBytes((ushort)1).CopyTo(spec, 4);       // keys
        BitConverter.GetBytes((ushort)1).CopyTo(spec, 16);      // position, from 1
        BitConverter.GetBytes((ushort)4).CopyTo(spec, 18);      // length
        BitConverter.GetBytes((ushort)0x0100).CopyTo(spec, 20); // extended type, unique
        spec[26] = 1;                                            // integer
        int len = spec.Length;
        Check(BTRCALL(CREATE, new byte[128], spec, ref len, Name(path), 255, 0), "create " + path);

        var pos = new byte[128];
        len = 0;
        Check(BTRCALL(OPEN, pos, new byte[1], ref len, Name(path), 255, 0), "open " + path);
        int updated = 0, deleted = 0;
        try {
            for (int id = 1; id <= count; id++) {
                var record = Record(id, false);
                len = record.Length;
                Check(BTRCALL(INSERT, pos, record, ref len, Key(id), 255, 0), "insert " + id);
            }
            var data = new byte[RecordLength];
            for (int id = 5; id <= count; id += 5) {
                len = data.Length;
                Check(BTRCALL(GET_EQUAL, pos, data, ref len, Key(id), 255, 0), "get " + id);
                var record = Record(id, true);
                len = record.Length;
                Check(BTRCALL(UPDATE, pos, record, ref len, Key(id), 255, 0), "update " + id);
                updated++;
            }
            for (int id = 7; id <= count; id += 7) {
                len = data.Length;
                Check(BTRCALL(GET_EQUAL, pos, data, ref len, Key(id), 255, 0), "get " + id);
                len = 0;
                Check(BTRCALL(DELETE, pos, data, ref len, Key(id), 255, 0), "delete " + id);
                deleted++;
            }
        } finally {
            len = 0;
            BTRCALL(CLOSE, pos, new byte[1], ref len, new byte[255], 255, 0);
        }
        return count + " inserted, " + updated + " updated, " + deleted + " deleted";
    }

    // The PAT pairs in the file, found as BtrieveDatabase::locatePATs does,
    // and whether pair 31 sits 4096 bytes past its place on the page grid.
    public static string Layout(string path, int pageSize) {
        var file = File.ReadAllBytes(path);
        long region = (long)(pageSize / 4) * pageSize;
        long expected = 2L * pageSize;
        int pairs = 0;
        string gap = "no PAT pair 31";
        for (int n = 1; expected + 2 * pageSize <= file.Length; n++) {
            long found = -1;
            for (long at = expected; at + 2 * pageSize <= file.Length; at += 512) {
                if (IsPATPair(file, at, pageSize, n)) { found = at; break; }
            }
            if (found < 0) break;
            if (n == 31) {
                gap = "PAT pair 31 is " + (found - expected) + " bytes past the page grid";
            }
            pairs = n;
            expected = found + region;
        }
        return file.Length + " bytes, " + pairs + " PAT pairs, " + gap;
    }

    static bool IsPATPair(byte[] file, long at, int pageSize, int n) {
        for (long page = at; page < at + 2 * pageSize; page += pageSize) {
            if (file[page] != 'P' || file[page + 1] != 'P' || BitConverter.ToUInt16(file, (int)page + 2) != n) {
                return false;
            }
        }
        return true;
    }

    public static void Stop() {
        int len = 0;
        BTRCALL(STOP, new byte[128], new byte[1], ref len, new byte[255], 255, 0);
    }
}
'@

# page size -> records: enough data pages (6 or 2 records each) for over 31.5
# PAT pairs' worth of logical pages, so the file runs past PAT pair 32
$files = [ordered]@{ 1536 = 72200; 512 = 8000 }
$made = @()
try {
    foreach ($pageSize in $files.Keys) {
        $path = Join-Path $Out "PATGAP$pageSize.DAT"
        if (Test-Path $path) { Remove-Item $path }
        $result = [PatGapDat]::Make($path, $pageSize, $files[$pageSize])
        Write-Output "$path`: $result"
        $made += , @($path, $pageSize)
    }
} finally {
    [PatGapDat]::Stop()
}
foreach ($m in $made) {
    Write-Output "$($m[0]): $([PatGapDat]::Layout($m[0], $m[1]))"
}
