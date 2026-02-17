' Parametro: periodo en formato PPP.YYYY (ej. 001.2026 = enero 2026)
' Exporta a Excel (.xlsx) via "Copy to XXL" (misma logica que compras_local para conexion SAP)
' Carpeta de salida: C:\Users\anad5004\Documents\Leoni_RPA
Option Explicit

Dim periodo, carpetaSalida, outFile
Dim SapGuiAuto, application, connection, session
Dim fso, shell, intentoConex, errGetObj, errDescObj, sapPath
Dim errEngine, errEngineDesc, errConn, nombresIntento, ni
Dim esperaSesion, maxEsperaSesion, intentoWnd, errWnd
Dim intentoOkcd, maxIntentosOkcd, errOkcd
Dim dlgFormato, dlg2, dlgR, tieneRuta, comboBox, pf1, pf2

' === CONFIGURACION SAP (P01 / Cliente 400) ===
Const SAP_SYSTEM = "P01"
Const SAP_CLIENT = "400"
Const SAP_CONNECTION_NAME = "P01"
Const SAP_LOGON_PATH = "C:\Program Files (x86)\SAP\FrontEnd\SAPgui\saplogon.exe"
Const MAX_INTENTOS_CONEXION = 3
Const ESPERA_ENTRE_INTENTOS = 10

carpetaSalida = "C:\Users\anad5004\Documents\Leoni_RPA"
If WScript.Arguments.Count < 1 Then
   WScript.Echo "Error: Debe proporcionar el periodo como argumento (formato PPP.YYYY, ej. 001.2026)."
   WScript.Quit 1
End If
periodo = Trim(WScript.Arguments(0))
If Len(periodo) = 0 Then
   WScript.Echo "Error: El periodo no puede estar vacio."
   WScript.Quit 1
End If

Set fso = CreateObject("Scripting.FileSystemObject")
Set shell = CreateObject("WScript.Shell")
If Not fso.FolderExists(carpetaSalida) Then
   fso.CreateFolder carpetaSalida
End If
' Nombre Excel: KE30_US10_PPP_YYYY.xlsx (ej. KE30_US10_001_2026.xlsx)
outFile = carpetaSalida & "\KE30_US10_" & Replace(periodo, ".", "_") & ".xlsx"

' Sin MsgBox ni dialogs que requieran usuario: todo el feedback en la plataforma web (servidor sin operador)

Sub Esperar(segundos)
   WScript.Sleep segundos * 1000
End Sub

' --- Excel: esperar que SAP abra Excel tras "Copy to XXL" ---
Function WaitForExcelApp(maxSeconds)
   Dim xlApp, i
   Set xlApp = Nothing
   For i = 1 To maxSeconds
      On Error Resume Next
      Set xlApp = GetObject(, "Excel.Application")
      On Error GoTo 0
      If Not xlApp Is Nothing Then
         Set WaitForExcelApp = xlApp
         Exit Function
      End If
      WScript.Sleep 1000
   Next
   Set WaitForExcelApp = Nothing
End Function

Sub DeleteIfExists(filePath)
   Dim fsoD
   Set fsoD = CreateObject("Scripting.FileSystemObject")
   On Error Resume Next
   If fsoD.FileExists(filePath) Then fsoD.DeleteFile filePath, True
   On Error GoTo 0
   Set fsoD = Nothing
End Sub

' --- Guardar exportacion Excel desde dialogo SAP (igual que compras_local): formato y Guardar como ---
Sub GuardarExportacionExcel(exportFolder, fileName)
   Dim sess
   Set sess = session
   Esperar 2
   On Error Resume Next
   Set dlgFormato = Nothing
   Err.Clear
   Set dlgFormato = sess.findById("wnd[1]")
   If Err.Number = 0 And Not (dlgFormato Is Nothing) Then
      tieneRuta = False
      Err.Clear
      Set pf1 = Nothing
      Set pf1 = sess.findById("wnd[1]/usr/ctxtDY_PATH")
      If Err.Number = 0 And Not (pf1 Is Nothing) Then tieneRuta = True
      Err.Clear
      If tieneRuta Then
         sess.findById("wnd[1]/usr/ctxtDY_PATH").text = exportFolder
         sess.findById("wnd[1]/usr/ctxtDY_FILENAME").text = fileName
         Esperar 1
         sess.findById("wnd[1]/tbar[0]/btn[11]").press
         Esperar 3
      Else
         Err.Clear
         sess.findById("wnd[1]/usr/radRB_OTHERS").select
         Esperar 1
         Err.Clear
         Set comboBox = sess.findById("wnd[1]/usr/cmbG_LISTBOX")
         If Err.Number = 0 And Not (comboBox Is Nothing) Then
            Err.Clear
            comboBox.key = "31"
            If Err.Number <> 0 Then Err.Clear : comboBox.key = "10"
         End If
         Err.Clear
         Esperar 1
         sess.findById("wnd[1]/tbar[0]/btn[0]").press
         If Err.Number <> 0 Then Err.Clear : sess.findById("wnd[1]").sendVKey 0
         Esperar 3
      End If
   End If
   Err.Clear
   Set dlg2 = Nothing
   Set dlg2 = sess.findById("wnd[1]")
   If Err.Number = 0 And Not (dlg2 Is Nothing) Then
      Err.Clear
      Set pf2 = Nothing
      Set pf2 = sess.findById("wnd[1]/usr/ctxtDY_PATH")
      If Err.Number = 0 And Not (pf2 Is Nothing) Then
         sess.findById("wnd[1]/usr/ctxtDY_PATH").text = exportFolder
         sess.findById("wnd[1]/usr/ctxtDY_FILENAME").text = fileName
         Esperar 1
         sess.findById("wnd[1]/tbar[0]/btn[11]").press
         Esperar 3
         Err.Clear
         Set dlgR = sess.findById("wnd[1]")
         If Err.Number = 0 And Not (dlgR Is Nothing) Then
            sess.findById("wnd[1]/tbar[0]/btn[11]").press
            Esperar 2
         End If
      Else
         sess.findById("wnd[1]").sendVKey 0
         Esperar 2
      End If
   End If
   Err.Clear
   Set dlgR = sess.findById("wnd[1]")
   If Err.Number = 0 And Not (dlgR Is Nothing) Then
      sess.findById("wnd[1]").sendVKey 0
      Esperar 2
   End If
   Err.Clear
   On Error GoTo 0
End Sub

' --- Cerrar instancias de Excel abiertas por Copy to XXL (igual que compras) ---
Sub CerrarExcelesAbiertos()
   Dim xlApp, intento, maxIntentos
   maxIntentos = 3
   For intento = 1 To maxIntentos
      On Error Resume Next
      Set xlApp = Nothing
      Set xlApp = GetObject(, "Excel.Application")
      If Err.Number <> 0 Or xlApp Is Nothing Then
         Err.Clear
         Exit Sub
      End If
      xlApp.DisplayAlerts = False
      While xlApp.Workbooks.Count > 0
         xlApp.Workbooks(1).Close False
      Wend
      xlApp.Quit
      Set xlApp = Nothing
      Err.Clear
      On Error GoTo 0
      Esperar 1
   Next
End Sub

Sub SaveLatestExcelAs(fullPathXlsx)
   Dim xlApp, wb
   Set xlApp = WaitForExcelApp(60)
   If xlApp Is Nothing Then
      WScript.Echo "ERROR: No se detecto Excel despues de Copy to XXL. Compruebe que no quedo un dialogo SAP abierto."
      Exit Sub
   End If
   xlApp.DisplayAlerts = False
   xlApp.Visible = False
   Set wb = xlApp.ActiveWorkbook
   wb.SaveAs fullPathXlsx, 51
   wb.Close False
   If xlApp.Workbooks.Count = 0 Then xlApp.Quit
   Set wb = Nothing
   Set xlApp = Nothing
End Sub

' --- FASE 1: Obtener SAP GUI ---
Set SapGuiAuto = Nothing
For intentoConex = 1 To MAX_INTENTOS_CONEXION
   On Error Resume Next
   Set SapGuiAuto = GetObject("SAPGUI")
   errGetObj = Err.Number
   errDescObj = Err.Description
   Err.Clear
   On Error GoTo 0
   If errGetObj = 0 And Not (SapGuiAuto Is Nothing) Then Exit For
   sapPath = ""
   If fso.FileExists(SAP_LOGON_PATH) Then
      sapPath = SAP_LOGON_PATH
   ElseIf fso.FileExists("C:\Program Files\SAP\FrontEnd\SAPgui\saplogon.exe") Then
      sapPath = "C:\Program Files\SAP\FrontEnd\SAPgui\saplogon.exe"
   ElseIf fso.FileExists("C:\Program Files (x86)\SAP\FrontEnd\SapGui\saplogon.exe") Then
      sapPath = "C:\Program Files (x86)\SAP\FrontEnd\SapGui\saplogon.exe"
   End If
   If sapPath <> "" Then
      shell.Run """" & sapPath & """", 1, False
   End If
   Esperar ESPERA_ENTRE_INTENTOS
Next
If SapGuiAuto Is Nothing Then
   WScript.Echo "ERROR: No se encontro SAP GUI. Abra SAP GUI manualmente (P01 / " & SAP_CLIENT & ")."
   WScript.Quit 1
End If

' --- FASE 2: Scripting Engine ---
On Error Resume Next
Set application = SapGuiAuto.GetScriptingEngine
errEngine = Err.Number
errEngineDesc = Err.Description
Err.Clear
On Error GoTo 0
If errEngine <> 0 Or application Is Nothing Then
   WScript.Echo "ERROR: Scripting Engine no disponible. Habilite Scripting en SAP GUI."
   WScript.Quit 1
End If

' --- FASE 3: Conexion P01 ---
If application.Children.Count > 0 Then
   Set connection = application.Children(0)
Else
   On Error Resume Next
   Set connection = application.OpenConnection(SAP_CONNECTION_NAME, True)
   errConn = Err.Number
   Err.Clear
   If errConn <> 0 Or connection Is Nothing Then
      nombresIntento = Array("R/3 - P01 - Production  ERP (SSO)", "P01 - Production", "P01 [1]", SAP_SYSTEM)
      For Each ni In nombresIntento
         Err.Clear
         Set connection = application.OpenConnection(ni, True)
         If Err.Number = 0 And Not connection Is Nothing Then Exit For
      Next
      Err.Clear
   End If
   On Error GoTo 0
   If connection Is Nothing Then
      WScript.Echo "ERROR: No se pudo conectar a SAP (P01). Verifique el nombre en SAP Logon."
      WScript.Quit 1
   End If
   Esperar 5
   maxEsperaSesion = 90
   For esperaSesion = 1 To maxEsperaSesion
      If connection.Children.Count > 0 Then Exit For
      Esperar 2
   Next
End If

' --- FASE 4: Sesion ---
If connection.Children.Count = 0 Then
   WScript.Echo "ERROR: No hay sesiones SAP. Inicie sesion en P01 / " & SAP_CLIENT & " y vuelva a ejecutar."
   WScript.Quit 1
End If
Set session = connection.Children(0)

Esperar 2
errWnd = -1
For intentoWnd = 1 To 20
   On Error Resume Next
   session.findById("wnd[0]").maximize
   errWnd = Err.Number
   Err.Clear
   On Error GoTo 0
   If errWnd = 0 Then Exit For
   Esperar 2
Next
If errWnd <> 0 Then
   WScript.Echo "ERROR: No se pudo acceder a la ventana de SAP. Esta ya logueado en P01?"
   WScript.Quit 1
End If
Esperar 1

' Esperar pantalla principal (campo de transaccion visible)
maxIntentosOkcd = 60
For intentoOkcd = 1 To maxIntentosOkcd
   On Error Resume Next
   session.findById("wnd[0]/tbar[0]/okcd").text = ""
   errOkcd = Err.Number
   Err.Clear
   On Error GoTo 0
   If errOkcd = 0 Then Exit For
   Esperar 2
Next
If errOkcd <> 0 Then
   WScript.Echo "ERROR: Pantalla principal de SAP no disponible despues de " & (maxIntentosOkcd * 2) & " seg. Inicie sesion en P01 y vuelva a ejecutar."
   WScript.Quit 1
End If

' Ejecutar transaccion KE30 (ventas) - igual que compras_local: /n + codigo + Enter
On Error Resume Next
session.findById("wnd[0]/tbar[0]/okcd").text = "/nke30"
session.findById("wnd[0]").sendVKey 0
If Err.Number <> 0 Then
   WScript.Echo "ERROR: No se pudo abrir transaccion KE30. Codigo: " & Err.Number & " - " & Err.Description
   WScript.Quit 1
End If
On Error GoTo 0
Esperar 3

session.findById("wnd[1]/usr/ctxtRKEA2-ERKRS").text = "us10"
session.findById("wnd[1]/usr/ctxtRKEA2-ERKRS").caretPosition = 4
session.findById("wnd[1]/tbar[0]/btn[0]").press
session.findById("wnd[0]/shellcont/shell").selectedNode = "000000001010"
session.findById("wnd[0]/shellcont/shell").doubleClickNode "000000001010"
session.findById("wnd[0]/usr/ctxtPAR_08").text = periodo
session.findById("wnd[0]/usr/ctxtPAR_09").text = periodo
session.findById("wnd[0]/usr/ctxtPAR_09").setFocus
session.findById("wnd[0]/usr/ctxtPAR_09").caretPosition = 8
session.findById("wnd[0]/tbar[1]/btn[8]").press
session.findById("wnd[0]/usr/lbl[1,3]").setFocus
session.findById("wnd[0]/usr/lbl[1,3]").caretPosition = 6
session.findById("wnd[0]").sendVKey 2
session.findById("wnd[0]/tbar[0]/btn[3]").press

' --- Exportar a Excel: Copy to XXL (en lugar de descarga .DAT) ---
session.findById("wnd[0]/tbar[1]/btn[48]").press
session.findById("wnd[1]/usr/btnD2000_PUSH_01").press
session.findById("wnd[1]/tbar[0]/btn[6]").press

' Opciones de columnas para la exportacion
session.findById("wnd[1]/usr/sub:SAPLKEC1:0100/chkCEC01-CHOICE[0,0]").selected = True
session.findById("wnd[1]/usr/sub:SAPLKEC1:0100/chkCEC01-CHOICE[1,0]").selected = True
session.findById("wnd[1]/usr/sub:SAPLKEC1:0100/chkCEC01-CHOICE[8,0]").selected = True
session.findById("wnd[1]/usr/sub:SAPLKEC1:0100/chkCEC01-CHOICE[10,0]").selected = True
session.findById("wnd[1]/usr/sub:SAPLKEC1:0100/chkCEC01-CHOICE[11,0]").selected = True
session.findById("wnd[1]/usr/sub:SAPLKEC1:0100/chkCEC01-CHOICE[11,0]").setFocus
session.findById("wnd[1]/usr").verticalScrollbar.position = 9
session.findById("wnd[1]/usr/sub:SAPLKEC1:0100/chkCEC01-CHOICE[3,0]").selected = True
session.findById("wnd[1]/usr/sub:SAPLKEC1:0100/chkCEC01-CHOICE[7,0]").selected = True
session.findById("wnd[1]/usr/sub:SAPLKEC1:0100/chkCEC01-CHOICE[7,0]").setFocus
session.findById("wnd[1]/usr").verticalScrollbar.position = 16
session.findById("wnd[1]/usr/sub:SAPLKEC1:0100/chkCEC01-CHOICE[10,0]").selected = True
session.findById("wnd[1]/usr/sub:SAPLKEC1:0100/chkCEC01-CHOICE[10,0]").setFocus
session.findById("wnd[1]/usr").verticalScrollbar.position = 18

session.findById("wnd[1]/tbar[0]/btn[0]").press

' Seleccionar "Copy to XXL" y confirmar
session.findById("wnd[1]/usr/subSUBSCREEN_STEPLOOP:SAPLSPO5:0150/sub:SAPLSPO5:0150/radSPOPLI-SELFLAG[0,0]").select
session.findById("wnd[1]/tbar[0]/btn[0]").press
session.findById("wnd[1]/tbar[0]/btn[0]").press

' Manejar dialogo SAP (formato / Guardar como) si aparece; si no, Excel se abrio y lo guardamos por COM
Esperar 2
GuardarExportacionExcel carpetaSalida, "KE30_US10_" & Replace(periodo, ".", "_") & ".xlsx"
Esperar 2
If Not fso.FileExists(outFile) Then
   DeleteIfExists outFile
   SaveLatestExcelAs outFile
End If
Esperar 2
CerrarExcelesAbiertos

' Sin MsgBox final: el resultado se muestra en la plataforma web
