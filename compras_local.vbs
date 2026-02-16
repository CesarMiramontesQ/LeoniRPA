' Parametros: fecha_inicio (YYYYMMDD), fecha_fin (YYYYMMDD) - pasados por linea de comandos
' Carpeta de salida fija: C:\Users\anad5004\Documents\Leoni_RPA
Dim fechaInicio, fechaFin, carpetaSalida
carpetaSalida = "C:\Users\anad5004\Documents\Leoni_RPA"
If WScript.Arguments.Count < 2 Then
   WScript.Echo "Error: Debe proporcionar fecha_inicio y fecha_fin como argumentos (YYYYMMDD)."
   WScript.Quit 1
End If
fechaInicio = WScript.Arguments(0)
fechaFin = WScript.Arguments(1)

If Not IsObject(application) Then
   Set SapGuiAuto  = GetObject("SAPGUI")
   Set application = SapGuiAuto.GetScriptingEngine
End If
If Not IsObject(connection) Then
   Set connection = application.Children(0)
End If
If Not IsObject(session) Then
   Set session    = connection.Children(0)
End If
If IsObject(WScript) Then
   WScript.ConnectObject session,     "on"
   WScript.ConnectObject application, "on"
End If
session.findById("wnd[0]").maximize
session.findById("wnd[0]/tbar[0]/okcd").text = "me80fn"
session.findById("wnd[0]").sendVKey 0
session.findById("wnd[0]/usr/ctxtSP$00006-HIGH").text = "US10"
session.findById("wnd[0]/usr/ctxtSP$00001-LOW").setFocus
session.findById("wnd[0]/usr/ctxtSP$00001-LOW").caretPosition = 0
session.findById("wnd[0]").sendVKey 4
session.findById("wnd[1]/usr/cntlCONTAINER/shellcont/shell").focusDate = fechaInicio
session.findById("wnd[1]/usr/cntlCONTAINER/shellcont/shell").selectionInterval = fechaInicio & "," & fechaInicio
session.findById("wnd[0]/usr/ctxtSP$00001-HIGH").setFocus
session.findById("wnd[0]/usr/ctxtSP$00001-HIGH").caretPosition = 0
session.findById("wnd[0]").sendVKey 4
session.findById("wnd[1]/usr/cntlCONTAINER/shellcont/shell").focusDate = fechaFin
session.findById("wnd[1]/usr/cntlCONTAINER/shellcont/shell").selectionInterval = fechaFin & "," & fechaFin
session.findById("wnd[0]/tbar[1]/btn[8]").press
session.findById("wnd[0]/usr/cntlMEALV_GRID_CONTROL_80FN/shellcont/shell").pressToolbarContextButton "&MB_EXPORT"
session.findById("wnd[0]/usr/cntlMEALV_GRID_CONTROL_80FN/shellcont/shell").selectContextMenuItem "&PC"
session.findById("wnd[1]/usr/subSUBSCREEN_STEPLOOP:SAPLSPO5:0150/sub:SAPLSPO5:0150/radSPOPLI-SELFLAG[1,0]").select
session.findById("wnd[1]/usr/subSUBSCREEN_STEPLOOP:SAPLSPO5:0150/sub:SAPLSPO5:0150/radSPOPLI-SELFLAG[1,0]").setFocus
session.findById("wnd[1]/tbar[0]/btn[0]").press
session.findById("wnd[1]/usr/ctxtDY_PATH").setFocus
session.findById("wnd[1]/usr/ctxtDY_PATH").caretPosition = 0
session.findById("wnd[1]").sendVKey 4
session.findById("wnd[2]/usr/ctxtDY_PATH").text = carpetaSalida
session.findById("wnd[2]/usr/ctxtDY_FILENAME").text = "compras_local.txt"
session.findById("wnd[2]/usr/ctxtDY_FILENAME").caretPosition = 17
session.findById("wnd[2]/tbar[0]/btn[0]").press
session.findById("wnd[1]/tbar[0]/btn[0]").press
session.findById("wnd[0]/usr/cntlMEALV_GRID_CONTROL_80FN/shellcont/shell").pressToolbarContextButton "DETAIL_MENU"
session.findById("wnd[0]/usr/cntlMEALV_GRID_CONTROL_80FN/shellcont/shell").selectContextMenuItem "TO_HIST"
session.findById("wnd[0]/usr/cntlMEALV_GRID_CONTROL_80FN_HIST/shellcont/shell").pressToolbarContextButton "&MB_EXPORT"
session.findById("wnd[0]/usr/cntlMEALV_GRID_CONTROL_80FN_HIST/shellcont/shell").selectContextMenuItem "&PC"
session.findById("wnd[1]/usr/subSUBSCREEN_STEPLOOP:SAPLSPO5:0150/sub:SAPLSPO5:0150/radSPOPLI-SELFLAG[1,0]").select
session.findById("wnd[1]/usr/subSUBSCREEN_STEPLOOP:SAPLSPO5:0150/sub:SAPLSPO5:0150/radSPOPLI-SELFLAG[1,0]").setFocus
session.findById("wnd[1]/tbar[0]/btn[0]").press
session.findById("wnd[1]/usr/ctxtDY_PATH").text = carpetaSalida
session.findById("wnd[1]/usr/ctxtDY_FILENAME").text = "historial_compras.txt"
session.findById("wnd[1]/usr/ctxtDY_FILENAME").caretPosition = 21
session.findById("wnd[1]/tbar[0]/btn[0]").press
