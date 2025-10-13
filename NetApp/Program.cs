using System.Runtime.InteropServices;

public partial class Program {
  [LibraryImport("wbtrv32.dll")]
  private static partial int BTRCALL(ushort wOperation, nint lpPositionBlock, nint lpDataBuffer,
                                     nint lpdwDataBufferLength, nint lpKeyBuffer, byte bKeyLength,
                                     byte sbKeyNumber);

  private static int managedBtrcall(ushort operation, byte[] posBlock, byte[] dataBuffer,
                                    ref int dwDataBufferLength, byte[] keyBuffer,
                                    byte sbKeyNumber) {
    IntPtr unmanagedPosBlock = Marshal.AllocHGlobal(128);
    IntPtr unmanagedDataBuffer = 0;
    IntPtr unmanagedDataBufferLength = Marshal.AllocHGlobal(sizeof(int));
    IntPtr unmanagedKeyBuffer = 0;
    byte keyBufferLength = 0;

    try {
      if (posBlock != null && posBlock.Length > 0) {
        Marshal.Copy(posBlock, 0, unmanagedPosBlock, posBlock.Length);
      }

      if (dataBuffer != null && dataBuffer.Length > 0) {
        unmanagedDataBuffer = Marshal.AllocHGlobal(dataBuffer.Length);
        Marshal.Copy(dataBuffer, 0, unmanagedDataBuffer, dataBuffer.Length);
      }

      int[] dataBufferLengthArray = new int[] { dwDataBufferLength };
      Marshal.Copy(dataBufferLengthArray, 0, unmanagedDataBufferLength, 1);

      if (keyBuffer != null && keyBuffer.Length > 0) {
        keyBufferLength = (byte)Math.Min(255, keyBuffer.Length);
        unmanagedKeyBuffer = Marshal.AllocHGlobal(keyBuffer.Length);
        Marshal.Copy(keyBuffer.ToArray(), 0, unmanagedKeyBuffer, keyBufferLength);
      }

      int response =
          BTRCALL(operation, unmanagedPosBlock, unmanagedDataBuffer, unmanagedDataBufferLength,
                  unmanagedKeyBuffer, keyBufferLength, sbKeyNumber);

      Marshal.Copy(unmanagedDataBufferLength, dataBufferLengthArray, 0, 1);
      dwDataBufferLength = dataBufferLengthArray[0];

      // posBlock could have changed, so return it
      if (posBlock != null) {
        Marshal.Copy(unmanagedPosBlock, posBlock, 0, 128);
      }

      // did we request data, if so return it
      if (dataBuffer != null && dwDataBufferLength > 0) {
        Marshal.Copy(unmanagedDataBuffer, dataBuffer, 0, dwDataBufferLength);
      }

      if (keyBuffer != null && keyBufferLength > 0) {
        Marshal.Copy(unmanagedKeyBuffer, keyBuffer, 0, keyBufferLength);
      }

      return response;
    } finally {
      Marshal.FreeHGlobal(unmanagedPosBlock);
      if (unmanagedDataBuffer != 0) {
        Marshal.FreeHGlobal(unmanagedDataBuffer);
      }
      Marshal.FreeHGlobal(unmanagedDataBufferLength);
      if (unmanagedKeyBuffer != 0) {
        Marshal.FreeHGlobal(unmanagedKeyBuffer);
      }
    }
  }

  public static void Main(string[] args) {
    byte[] posBlock = new byte[128];
    int dwDataBufferLength = 0;
    Console.WriteLine("Opening " +
                      managedBtrcall(0, posBlock, null, ref dwDataBufferLength,
                                     System.Text.Encoding.ASCII.GetBytes("c:\\bbsv10\\wgserv2.dat"),
                                     0));

    byte[] dataBuffer = new byte[8192];
    byte[] keyBuffer = new byte[256];
    dwDataBufferLength = dataBuffer.Length;
    managedBtrcall(0xC, posBlock, dataBuffer, ref dwDataBufferLength, keyBuffer, 0);

    Console.WriteLine("Closing " +
                      managedBtrcall(1, posBlock, null, ref dwDataBufferLength, null, 0));
  }

  /*  auto mbbsEmuDb = tempPath->copyToTempPath("assets/MBBSEMU.DB");
ASSERT_FALSE(mbbsEmuDb.empty());

DWORD dwDataBufferLength = 0;

ASSERT_EQ(btrcall(btrieve::OperationCode::Open, posBlock, nullptr,
                  &dwDataBufferLength,
                  const_cast<LPVOID>(reinterpret_cast<LPCVOID>(
                      toStdString(mbbsEmuDb.c_str()).c_str())),
                  -1, 0),
          btrieve::BtrieveError::Success);

ASSERT_EQ(btrcall(btrieve::OperationCode::Close, posBlock, nullptr,
                  &dwDataBufferLength, nullptr, 0, 0),
          btrieve::BtrieveError::Success);
ASSERT_EQ(btrcall(btrieve::OperationCode::Close, posBlock, nullptr,
                  &dwDataBufferLength, nullptr, 0, 0),
          btrieve::BtrieveError::FileNotOpen); */
}
