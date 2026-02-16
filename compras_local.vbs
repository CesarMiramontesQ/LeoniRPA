' Parametros: fecha_inicio (YYYYMMDD), fecha_fin (YYYYMMDD) - pasados por linea de comandos
' Exporta a Excel (.xlsx) usando &XXL y manejo de dialogos (referencia exportar-sap.vbs)
' Carpeta de salida: C:\Users\anad5004\Documents\Leoni_RPA (o crear exports junto al script)
Option Explicit

Dim fechaInicio, fechaFin, carpetaSalida
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
If WScript.Arguments.Count < 2 Then
   WScript.Echo "Error: Debe proporcionar fecha_inicio y fecha_fin como argumentos (YYYYMMDD)."
   WScript.Quit 1
End If
fechaInicio = WScript.Arguments(0)
fechaFin = WScript.Arguments(1)

Set fso = CreateObject("Scripting.FileSystemObject")
Set shell = CreateObject("WScript.Shell")
If Not fso.FolderExists(carpetaSalida) Then
   fso.CreateFolder carpetaSalida
End If

' Modal inicial: avisar que va a procesar
MsgBox "Procesando descarga de compras (ME80FN) a Excel." & vbCrLf & vbCrLf & "Haga clic en Aceptar para iniciar. No cierre SAP hasta que vea el mensaje de finalización.", vbInformation + vbOKOnly, "Leoni RPA - Compras"

Sub Esperar(segundos)
   WScript.Sleep segundos * 1000
End Sub

' --- Guardar exportacion Excel: maneja dialogo de formato (si aparece) y dialogo Guardar como ---
Sub GuardarExportacionExcel(exportFolder, fileName)
   Dim sess
   Set sess = session
   Esperar 2
   On Error Resume Next
   ' Dialogo 1: puede ser formato (Select Spreadsheet) o guardar directo
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
         ' Dialogo de formato: elegir Excel (B1 Excel Open XML)
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
   ' Dialogo 2: Guardar archivo (si aun no se lleno)
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
         ' Confirmar reemplazo si existe archivo
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
   ' Esperar a que exista sesion (login manual o SSO)
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

' (No usar ConnectObject: igual que exportar-sap.vbs, para evitar bloqueos)

' Esperar a que la ventana principal (wnd[0]) exista
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
   WScript.Echo "ERROR: No se pudo acceder a la ventana de SAP. ¿Esta ya logueado en P01?"
   WScript.Quit 1
End If
Esperar 1

' CLAVE: Esperar a que la PANTALLA PRINCIPAL este cargada (campo de transaccion visible).
' Si aun esta en pantalla de logon, okcd no existe y el script fallaria aqui.
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

' Ejecutar transaccion ME80FN (compras) - igual que exportar-sap: /n + codigo + Enter
On Error Resume Next
session.findById("wnd[0]/tbar[0]/okcd").text = "/nme80fn"
session.findById("wnd[0]").sendVKey 0
If Err.Number <> 0 Then
   WScript.Echo "ERROR: No se pudo abrir transaccion me80fn. Codigo: " & Err.Number & " - " & Err.Description
   WScript.Quit 1
End If
On Error GoTo 0
Esperar 3

session.findById("wnd[0]/usr/ctxtSP$00006-LOW").text = "MX10"
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
Esperar 3
' --- Exportar lista actual a Excel (&XXL) ---
session.findById("wnd[0]/usr/cntlMEALV_GRID_CONTROL_80FN/shellcont/shell").pressToolbarContextButton "&MB_EXPORT"
session.findById("wnd[0]/usr/cntlMEALV_GRID_CONTROL_80FN/shellcont/shell").selectContextMenuItem "&XXL"
GuardarExportacionExcel carpetaSalida, "compras_local_" & fechaInicio & "_" & fechaFin & ".xlsx"
' --- Ir a historial y exportar a Excel ---
session.findById("wnd[0]/usr/cntlMEALV_GRID_CONTROL_80FN/shellcont/shell").pressToolbarContextButton "DETAIL_MENU"
session.findById("wnd[0]/usr/cntlMEALV_GRID_CONTROL_80FN/shellcont/shell").selectContextMenuItem "TO_HIST"
session.findById("wnd[0]/usr/cntlMEALV_GRID_CONTROL_80FN_HIST/shellcont/shell").pressToolbarContextButton "&MB_EXPORT"
session.findById("wnd[0]/usr/cntlMEALV_GRID_CONTROL_80FN_HIST/shellcont/shell").selectContextMenuItem "&XXL"
GuardarExportacionExcel carpetaSalida, "historial_compras_" & fechaInicio & "_" & fechaFin & ".xlsx"

' Modal final: avisar que el proceso terminó
MsgBox "Proceso terminado correctamente." & vbCrLf & vbCrLf & "Archivos Excel guardados en:" & vbCrLf & carpetaSalida & vbCrLf & vbCrLf & "• compras_local_" & fechaInicio & "_" & fechaFin & ".xlsx" & vbCrLf & "• historial_compras_" & fechaInicio & "_" & fechaFin & ".xlsx", vbInformation + vbOKOnly, "Leoni RPA - Compras"
