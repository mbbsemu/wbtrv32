// dllmain.cpp : Defines the entry point for the DLL application.
#include "framework.h"

// #define DEBUG_ATTACH

#ifdef DEBUG_ATTACH
#include "psapi.h"
#endif

BOOL APIENTRY DllMain(HMODULE hModule, DWORD ul_reason_for_call,
                      LPVOID lpReserved) {
  TCHAR buf[MAX_PATH];
  switch (ul_reason_for_call) {
    case DLL_PROCESS_ATTACH:
#ifdef DEBUG_ATTACH
      GetProcessImageFileName(GetCurrentProcess(), buf, ARRAYSIZE(buf));
      MessageBox(NULL, TEXT("Attach now"), buf, MB_OK);
#else
        ;
#endif
      break;
    case DLL_THREAD_ATTACH:
    case DLL_THREAD_DETACH:
    case DLL_PROCESS_DETACH:
      break;
  }
  return TRUE;
}
