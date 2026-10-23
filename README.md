
# WBTRV32.DLL: A Drop-in Replacement for Pervasive Btrieve Using SQLite
## Table of Contents
-   [Introduction](#introduction)
-   [Features](#features)
-   [Getting Started](#getting-started)
    -   [Prerequisites](#prerequisites)
    -   [Installation](#installation)
-   [Usage](#usage)
-   [Development and Testing](#development-and-testing)
    -   [Building from Source](#building-from-source)
    -   [Unit Tests](#unit-tests)
-   [Supported Versions](#supported-versions)
-   [Compatibility](#compatibility)
-   [Contributing](#contributing)
-   [License](#license)

## Introduction
**WBTRV32.DLL** is an open-source, drop-in replacement for the Pervasive Btrieve `WBTRV32.DLL` library, providing a modern solution for legacy applications that rely on antiquated versions of Btrieve (versions 5 and 6). By emulating the exact DLL signature of the original Btrieve library, this replacement allows existing applications to operate without modification.

Instead of interfacing directly with Btrieve `.DAT` files, this replacement DLL converts the data to a SQLite backend. This approach offers several advantages:
- **Cross-Platform Compatibility**: Run your applications on Windows, Linux, macOS, and other platforms supported by SQLite.
- **Modern Database Support**: Benefit from the reliability, performance, and ongoing support of SQLite.
- **Seamless Integration**: Replace the original DLL without altering your application's codebase.

Originally developed as part of [The Major BBS Emulation Project](https://github.com/mbbsemu), this library is now available for any software confined to outdated Btrieve versions, extending the lifespan and usability of legacy applications.

## Features

- **Drop-in Replacement**: Mimics the original `WBTRV32.DLL` interface, requiring no changes to application code.
- **Built with Modern C++**: Developed entirely in modern C++, leveraging the latest language features for efficiency and maintainability.
- **SQLite Backend**: Converts Btrieve `.DAT` files to a SQLite database, leveraging a modern and widely supported database engine.
- **Comprehensive Unit Tests**: Includes unit tests covering many Btrieve data scenarios to ensure reliability and correctness.
- **Cross-Platform Support**: Enables applications to run on multiple operating systems including Windows, Linux, and macOS.
- **Supports Btrieve Versions 5 and 6**: Ensures compatibility with applications using these legacy versions.
- **Open Source**: Encourages community contributions and transparency.
- **Automated Data Conversion**: Handles the migration of data from Btrieve to SQLite seamlessly.
- **Logging and Diagnostics**: Provides detailed logs to assist with troubleshooting and monitoring.

## Getting Started
### Prerequisites
- **Operating System**:
    - **Windows**: Windows 7 or later.
    - **Linux**: Any modern distribution.
    - **macOS**: Versions supporting your application and SQLite.
- **Development Tools** (if building from source):
    - [Bazel](https://bazel.build/) (via [Bazelisk](https://github.com/bazelbuild/bazelisk) is recommended) drives the build on all platforms.
    - A C++20-capable compiler. Bazel will fetch hermetic toolchains for cross-compilation automatically (see below); a native compiler (GCC/Clang on Linux/macOS, or Visual Studio's MSVC on Windows) is only needed to build for your host platform.
    - Windows builds can alternatively be produced with Visual Studio 2022 or newer via `vstudio/vstudio.sln`, which needs [vcpkg](https://vcpkg.io) (included with Visual Studio's C++ workload) for Abseil.

### Installation
1. **Backup Original DLL**:
    - Locate the original `WBTRV32.DLL` in your application's directory.
    - Create a backup copy to prevent data loss.
2. **Download Replacement DLL**:
    - Obtain the pre-built `WBTRV32.DLL` package (which includes SQLite) from the [Releases](https://github.com/mbbsemu/wbtrv32/releases) section.
    - Alternatively, clone the repository and build from source.
3. **Replace DLL**:
    -   Copy the new `WBTRV32.DLL` along with with supporting files into your application's directory, replacing the original.
4. **Run Application**:
    -   Launch your application as usual.
    -   The DLL will handle data conversion and operations transparently.

## Usage
- **First Run Data Conversion**:
    - The DLL automatically converts existing Btrieve `.DAT` files to a SQLite database on first access.
    - This process is fast and completely transparent. 
    - Your original `.DAT` files are left in place and unmodified.
- **Application Operation**:
    - The application should function normally, with database operations redirected to SQLite.
    - No code changes are required in the application.
- **Database Management**:
    -   Use standard SQLite tools for database maintenance and inspection.
    -   Backup the SQLite database regularly to prevent data loss.
- **Logging**:
    - Problems go to `wbtrv32.log` in the current directory (`/tmp/wbtrv32.log` outside Windows), after a first line naming the build and the program that loaded it. Logging uses the [Abseil logging library](https://abseil.io/docs/cpp/guides/logging); each line starts with its severity (`I`nfo, `W`arning, `E`rror), local date and time, thread and source line, e.g. `W 2026-10-22 11:08:37.573535 4242 wbtrv32.cpp:1788] [C:\BBSV10\BBSUSR.DAT]: handled Update [key 0], returned DuplicateKeyValue`. With a debugger attached on Windows, lines also go to its output window.
    - Set how much with `LogLevel=` under `[wbtrv32]` in `wbtrv32.ini` in the current directory, or the `WBTRV32_LOGLEVEL` environment variable, which wins: `0` nothing, `1` problems only (the default), `2` every non-zero status, including the routine key not found (4), end of file (9) and short buffer (22), `3` every open as well.
- **Debugging**:
    - On Windows, `DebugAttach=1` under `[wbtrv32]` in `wbtrv32.ini`, or the `WBTRV32_DEBUGATTACH=1` environment variable, pops up a message box when the DLL loads, so you can attach a debugger before it does anything else.
- **Creating Databases from Description Files**:
    - `create_database` is the equivalent of `butil -create`: it reads a Btrieve [description file](https://docs.actian.com/psql/psqlv13/index.html#page/advops%2Fdescfile.htm) (`.BCR`) and issues a Btrieve Create through wbtrv32, producing an empty `.db` database.
    - ```
      bazel build //util:create_database
      bazel-bin/util/create_database [--overwrite] <outputFile> <descriptionFile>
      ```
    - ACS files named with `name=` are resolved relative to the description file.

## Development and Testing

### Building from Source

The project is built with [Bazel](https://bazel.build/) and developed entirely in modern C++ (C++20). SQLite is vendored directly in the [`sqlite/`](sqlite/) directory, so no external SQLite installation is required.

#### Prerequisites
- **Bazel**: Install via [Bazelisk](https://github.com/bazelbuild/bazelisk), which reads the project's pinned Bazel version automatically.
- A native C++ compiler is only needed when building for your host platform; cross-compilation targets use Bazel-managed hermetic toolchains (`gcc_toolchain` for Linux arm32/arm64, `hermetic_cc_toolchain` (zig-cc) for Windows and macOS) that Bazel downloads on demand.

#### Build Steps
1. **Clone the Repository**:
```
git clone https://github.com/mbbsemu/wbtrv32.git
cd wbtrv32
```

2. **Build for your host platform**:
```
bazel build //vstudio/wbtrv32:wbtrv32
```
    The resulting shared library (`wbtrv32.dll`, `wbtrv32.dylib`, or `wbtrv32.so` depending on platform) is written under `bazel-bin/vstudio/wbtrv32/`.

3. **Cross-compile for another platform** using one of the configs defined in `.bazelrc`:
```
bazel build --config=linux_arm64   //vstudio/wbtrv32:wbtrv32
bazel build --config=linux_arm32   //vstudio/wbtrv32:wbtrv32
bazel build --config=windows_amd64 //vstudio/wbtrv32:wbtrv32
bazel build --config=windows_x86   //vstudio/wbtrv32:wbtrv32
bazel build --config=windows_arm64 //vstudio/wbtrv32:wbtrv32
bazel build --config=macos_amd64   //vstudio/wbtrv32:wbtrv32
bazel build --config=macos_arm64   //vstudio/wbtrv32:wbtrv32
```
    This makes it possible to produce the Windows `WBTRV32.DLL` (and macOS/Linux equivalents) from a single Linux or macOS host without any additional toolchain installation.

4. **Build the NuGet package** (bundles every supported runtime into a single package):
```
bazel build //nuget:package
```
    The `.nupkg` is written to `bazel-bin/nuget/package/`.

5. **Windows via Visual Studio** (alternative to Bazel on Windows):
    - Open `vstudio/vstudio.sln` in Visual Studio 2022 or newer and build the `wbtrv32` project. This is the same solution used by the project's CI.
    - The `wbtrv32` project gets [Abseil](https://abseil.io) from vcpkg in manifest mode (`vcpkg.json` at the top of the repository), building it on first use. If you've never used vcpkg with MSBuild, run `vcpkg integrate install` once from a Developer Command Prompt.

6. **Bumping the DLL version**: the version shown in the DLL's Properties > Details lives in `nuget/BUILD`, alongside the NuGet package version it shares. Bazel builds stamp it into the DLL directly; the Visual Studio build reads the checked-in `vstudio/wbtrv32/wbtrv32_version.h`, so after editing it refresh that copy with:
```
bazel run //vstudio/wbtrv32:update_version
```
    `bazel test //...` fails if you forget.

### Unit Tests
Comprehensive unit tests are included to verify functionality across various Btrieve data scenarios.

#### Running Tests
Run the full test suite with Bazel:
```
bazel test //...
```
Or target a specific test suite, e.g.:
```
bazel test //vstudio/wbtrv32:tests
bazel test //btrieve:tests
```
Test data files (sample `.DAT`/`.DB` Btrieve databases) are provided in [`assets/`](assets/) and are pulled in automatically as test data by Bazel.

**Continuous Integration**: GitHub Actions builds and runs the test suite on Windows via MSBuild/`vstudio.sln` on every push and pull request to `main` (see [`.github/workflows/msbuild.yml`](.github/workflows/msbuild.yml)).

## Supported Versions
- **Btrieve Version 5**: Full support for data files and operations.
- **Btrieve Version 6**: Full support for data files and operations.

## Compatibility
- **API Coverage**:
    - Implements core Btrieve API functions used by most applications.
    - Aims for behavioral parity with the original DLL.

## Contributing
We welcome contributions to enhance this project:
- **Reporting Issues**:
    - Use the [Issues](https://github.com/mbbsemu/wbtrv32/issues) tab to report bugs or request features.
    - Provide detailed descriptions and steps to reproduce issues.
- **Submitting Pull Requests**:
    - Fork the repository.
    - Create a feature branch (`git checkout -b feature/YourFeature`).
    - Commit your changes with clear messages.
    - Ensure that all unit tests pass and include new tests if applicable.
    - Push to your fork and submit a pull request.
- **Community Engagement**:
	- Join the discussion on our [Discord](https://discord.gg/BgjxMD5)!
    - Share insights and usage experiences.

## License

This project is licensed under the MIT License. You are free to use, modify, and distribute this software under the terms of the license.
