
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
    - **Windows**: Visual Studio 2022 or newer.
    - **Linux/macOS**: GCC 7.1+, Clang 5.0+, or equivalent with C++17 support.

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

## Development and Testing

### Building from Source

The project is developed entirely in modern C++, utilizing C++17 features for improved performance and code clarity.

#### Prerequisites
- **C++ Compiler**:
    - **Windows**: Visual Studio 2017 or later, or MinGW-w64 with GCC 7.1+.
    - **Linux/macOS**: GCC 7.1+ or Clang 5.0+.
- **CMake**: Version 3.10 or later.
- **SQLite Development Libraries**: Ensure the SQLite development headers and libraries are available.

#### Build Steps
1. **Clone the Repository**:
```
git clone https://github.com/mbbsemu/wbtrv3t.git
cd wbtrv3t
``` 
    
2. **Create Build Directory**:
```
mkdir build && cd build
```
    
3. **Configure the Build with CMake**:
```
cmake .. -DCMAKE_BUILD_TYPE=Release
``` 
    
5.  **Build the Project**:
    - **Windows**:
        ```
        cmake --build . --config Release
        ```
    - **Linux/macOS**:
        ```
        make
        ``` 
6.  **Output**:
    - The compiled `WBTRV32.DLL` (or equivalent shared library) will be located in the build output directory.

### Unit Tests
Comprehensive unit tests are included to verify functionality across various Btrieve data scenarios.

#### Running Tests
1. **Build Tests**:
    -   Ensure that the `BUILD_TESTS` option is enabled during the CMake configuration:
```
cmake .. -DBUILD_TESTS=ON
```
2. **Compile**:
    - Build the test suite along with the main project.
3. **Execute Tests**:
    - Run the test executable generated during the build process:
`./wbtrv32_tests` 
    - Review the test results to ensure all tests pass.
4. **Continuous Integration**:
    - The project may include CI configurations (e.g., GitHub Actions) for automated testing on different platforms.

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
