// dllmain.cpp : Defines the entry point for the DLL application.
#include "framework.h"
#include "wbtrv32.h"

// #define DEBUG_ATTACH

#ifdef DEBUG_ATTACH
#include "psapi.h"
#endif

BOOL APIENTRY DllMain(HMODULE hModule, DWORD ul_reason_for_call,
                      LPVOID lpReserved) {
  switch (ul_reason_for_call) {
    case DLL_PROCESS_ATTACH:
#ifdef DEBUG_ATTACH
    {
      TCHAR buf[MAX_PATH];

      GetProcessImageFileName(GetCurrentProcess(), buf, ARRAYSIZE(buf));
      MessageBox(NULL, TEXT("Attach now"), buf, MB_OK);
    }
#endif
      wbtrv32::processAttach();
      break;
    case DLL_PROCESS_DETACH:
      wbtrv32::processDetach();
      break;
    case DLL_THREAD_ATTACH:
    case DLL_THREAD_DETACH:
      break;
  }
  return TRUE;
}
