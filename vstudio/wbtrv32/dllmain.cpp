// dllmain.cpp : Defines the entry point for the DLL application.
#include "framework.h"
#include "wbtrv32.h"

BOOL APIENTRY DllMain(HMODULE hModule, DWORD ul_reason_for_call,
                      LPVOID lpReserved) {
  switch (ul_reason_for_call) {
    case DLL_PROCESS_ATTACH:
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
