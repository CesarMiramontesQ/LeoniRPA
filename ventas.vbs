' Parametro: periodo en formato PPP.YYYY (ej. 001.2026 = enero 2026)
' Exporta a Excel (.xlsx) via "Copy to XXL" (misma logica que compras_local para conexion SAP)
' Carpeta de salida: C:\Users\anad5004\Documents\Leoni_RPA
' Sin modales Windows: el feedback se muestra solo en la UX de la app.
Option Explicit

Dim periodo, carpetaSalida, outFile
Dim SapGuiAuto, application, connection, session
Dim fso, shell, intentoConex, errGetObj, errDescObj, sapPath
Dim errEngine, errEngineDesc, errConn, errConnDesc, nombresEntrada, nombreEntrada
Dim esperaSesion, maxEsperaSesion, intentoWnd, errWnd
Dim intentoOkcd, maxIntentosOkcd, errOkcd

' === CONFIGURACION SAP (P01 / Cliente 400) ===
Const SAP_SYSTEM = "P01"
Const SAP_CLIENT = "400"
Const SAP_CONNECTION_NAME = "R/3 - P01 - Production LCS"
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

Sub Esperar(segundos)
   WScript.Sleep segundos * 1000
End Sub

Sub Log(mensaje)
   WScript.Echo "[ventas] " & Now & " - " & mensaje
End Sub

' Salida con codigo (usada por la conexion/login SAP).
Sub Salir(codigo)
   WScript.Quit codigo
End Sub

' Lee una clave del entorno del proceso y, si no existe, del archivo .env
' junto al script o en el directorio de trabajo. No escribe el valor en el log.
Function LeerVariableEnv(clave)
   Dim valor, rutaScript, rutaCwd
   valor = ""
   On Error Resume Next
   valor = Trim(shell.Environment("PROCESS")(clave) & "")
   Err.Clear
   On Error GoTo 0
   If Len(valor) > 0 Then
      LeerVariableEnv = valor
      Exit Function
   End If

   rutaScript = fso.GetParentFolderName(WScript.ScriptFullName) & "\.env"
   rutaCwd = fso.GetAbsolutePathName(".\.env")
   If fso.FileExists(rutaScript) Then
      valor = LeerClaveArchivoEnv(rutaScript, clave)
   End If
   If Len(valor) = 0 And StrComp(rutaScript, rutaCwd, 1) <> 0 And fso.FileExists(rutaCwd) Then
      valor = LeerClaveArchivoEnv(rutaCwd, clave)
   End If
   LeerVariableEnv = valor
End Function

Function LeerClaveArchivoEnv(ruta, clave)
   Dim stream, texto, lineas, i, linea, pos, k, v
   LeerClaveArchivoEnv = ""
   On Error Resume Next
   Set stream = CreateObject("ADODB.Stream")
   stream.Type = 2
   stream.Charset = "utf-8"
   stream.Open
   stream.LoadFromFile ruta
   texto = stream.ReadText
   stream.Close
   If Err.Number <> 0 Then
      Err.Clear
      On Error GoTo 0
      Exit Function
   End If
   On Error GoTo 0
   texto = Replace(texto, vbCrLf, vbLf)
   texto = Replace(texto, vbCr, vbLf)
   lineas = Split(texto, vbLf)
   For i = 0 To UBound(lineas)
      linea = Trim(lineas(i))
      If Len(linea) = 0 Then
         ' linea vacia
      ElseIf Left(linea, 1) = "#" Then
         ' comentario
      Else
         pos = InStr(linea, "=")
         If pos > 1 Then
            k = Trim(Left(linea, pos - 1))
            v = Trim(Mid(linea, pos + 1))
            If Len(v) >= 2 Then
               If (Left(v, 1) = """" And Right(v, 1) = """") Or (Left(v, 1) = "'" And Right(v, 1) = "'") Then
                  v = Mid(v, 2, Len(v) - 2)
               End If
            End If
            If StrComp(k, clave, 1) = 0 Then
               LeerClaveArchivoEnv = v
               Exit Function
            End If
         End If
      End If
   Next
End Function

Function EsPantallaLoginSAP()
   EsPantallaLoginSAP = ObjetoExiste("wnd[0]/usr/txtRSYST-BNAME") And ObjetoExiste("wnd[0]/usr/pwdRSYST-BCODE")
End Function

' Compara nombres de entradas de SAP Logon sin importar mayusculas ni espacios repetidos.
Function NormalizarNombre(txt)
   Dim r
   r = Trim(txt & "")
   Do While InStr(r, "  ") > 0
      r = Replace(r, "  ", " ")
   Loop
   NormalizarNombre = LCase(r)
End Function

Function InfoSesion(ses, campo)
   Dim v
   v = ""
   On Error Resume Next
   Select Case campo
      Case "SystemName": v = ses.Info.SystemName
      Case "Client": v = ses.Info.Client
      Case "User": v = ses.Info.User
   End Select
   Err.Clear
   On Error GoTo 0
   InfoSesion = UCase(Trim(v & ""))
End Function

' Busca una sesion abierta de P01 que se pueda reutilizar:
'   - ya logueada en cliente 400, o
'   - en la pantalla de acceso de la entrada SAP_CONNECTION_NAME (sin SSO).
' Deja connection y session asignadas si la encuentra.
Function BuscarSesionP01()
   Dim idxCon, idxSes, con, ses, sesLogin, conLogin, esBusy
   BuscarSesionP01 = False
   Set sesLogin = Nothing
   Set conLogin = Nothing
   On Error Resume Next
   For idxCon = 0 To application.Children.Count - 1
      Set con = application.Children(idxCon)
      For idxSes = 0 To con.Children.Count - 1
         Set ses = con.Children(idxSes)
         esBusy = False
         esBusy = ses.Busy
         If Not esBusy And InfoSesion(ses, "SystemName") = SAP_SYSTEM Then
            If Len(InfoSesion(ses, "User")) > 0 Then
               If InfoSesion(ses, "Client") = SAP_CLIENT Then
                  Set connection = con
                  Set session = ses
                  BuscarSesionP01 = True
                  Err.Clear
                  On Error GoTo 0
                  Exit Function
               End If
            ElseIf sesLogin Is Nothing And NormalizarNombre(con.Description) = NormalizarNombre(SAP_CONNECTION_NAME) Then
               Set conLogin = con
               Set sesLogin = ses
            End If
         End If
      Next
   Next
   Err.Clear
   On Error GoTo 0
   If Not (sesLogin Is Nothing) Then
      Set connection = conLogin
      Set session = sesLogin
      BuscarSesionP01 = True
   End If
End Function

' Pantalla de acceso: cliente 400, usuario y contrasena. Sin SSO ni SNC.
Sub IniciarSesionSAP()
   Dim sapUser, sapPass, sapLang
   If Not EsPantallaLoginSAP() Then Exit Sub

   sapUser = LeerVariableEnv("SAP_USER")
   sapPass = LeerVariableEnv("SAP_PASSWORD")
   sapLang = LeerVariableEnv("SAP_LANGUAGE")
   If Len(sapUser) = 0 Or Len(sapPass) = 0 Then
      WScript.Echo "ERROR: SAP pide usuario y contrasena. Define SAP_USER y SAP_PASSWORD en el archivo .env (no las dejes en el script)."
      Salir 1
   End If

   Log "Pantalla de acceso SAP detectada. Iniciando sesion en " & SAP_SYSTEM & " / cliente " & SAP_CLIENT & "..."
   On Error Resume Next
   session.findById("wnd[0]/usr/txtRSYST-MANDT").text = SAP_CLIENT
   Err.Clear
   session.findById("wnd[0]/usr/txtRSYST-BNAME").text = sapUser
   session.findById("wnd[0]/usr/pwdRSYST-BCODE").text = sapPass
   If Len(sapLang) > 0 Then session.findById("wnd[0]/usr/txtRSYST-LANGU").text = sapLang
   session.findById("wnd[0]/usr/pwdRSYST-BCODE").setFocus
   session.findById("wnd[0]").sendVKey 0
   If Err.Number <> 0 Then
      WScript.Echo "ERROR: No se pudo enviar el inicio de sesion SAP. " & Err.Number & " - " & Err.Description
      Err.Clear
      On Error GoTo 0
      sapUser = ""
      sapPass = ""
      Salir 1
   End If
   Err.Clear
   On Error GoTo 0
   sapUser = ""
   sapPass = ""
   EsperarDialogoPostLogin
End Sub

' Espera la pantalla principal. Atiende aviso de sesion multiple y mensajes informativos.
Sub EsperarDialogoPostLogin()
   Dim i, msgTxt, msgType
   For i = 1 To 30
      If Not EsPantallaLoginSAP() And ObjetoExiste("wnd[0]/tbar[0]/okcd") And Not ObjetoExiste("wnd[1]") Then
         Log "Sesion SAP lista."
         Exit Sub
      End If
      If ObjetoExiste("wnd[1]/usr/radMULTI_LOGON_OPT2") Then
         Log "SAP informa sesion multiple. Se continua sin cerrar las otras sesiones."
         On Error Resume Next
         session.findById("wnd[1]/usr/radMULTI_LOGON_OPT2").select
         session.findById("wnd[1]/tbar[0]/btn[0]").press
         Err.Clear
         On Error GoTo 0
         Esperar 2
      ElseIf ObjetoExiste("wnd[1]/tbar[0]/btn[0]") Then
         On Error Resume Next
         session.findById("wnd[1]/tbar[0]/btn[0]").press
         Err.Clear
         On Error GoTo 0
         Esperar 1
      ElseIf ObjetoExiste("wnd[1]") Then
         On Error Resume Next
         session.findById("wnd[1]").sendVKey 0
         Err.Clear
         On Error GoTo 0
         Esperar 1
      ElseIf EsPantallaLoginSAP() Then
         msgTxt = ObtenerTextoBarraEstado()
         msgType = ObtenerTipoBarraEstado()
         If (msgType = "E" Or msgType = "A") And Len(msgTxt) > 0 Then
            WScript.Echo "ERROR: SAP rechazo el inicio de sesion: " & msgTxt
            Salir 1
         End If
         Esperar 1
      Else
         Esperar 1
      End If
   Next
End Sub

Function ObjetoExiste(objId)
   Dim objTmp
   On Error Resume Next
   Set objTmp = Nothing
   Err.Clear
   Set objTmp = session.findById(objId)
   ObjetoExiste = (Err.Number = 0 And Not (objTmp Is Nothing))
   Err.Clear
   On Error GoTo 0
End Function

Function ObtenerTextoBarraEstado()
   Dim txt
   txt = ""
   On Error Resume Next
   Err.Clear
   txt = session.findById("wnd[0]/sbar").Text
   If Err.Number <> 0 Or Len(Trim(txt)) = 0 Then
      Err.Clear
      txt = session.findById("wnd[0]/sbar/pane[0]").Text
   End If
   Err.Clear
   On Error GoTo 0
   ObtenerTextoBarraEstado = Trim(txt)
End Function

Function ObtenerTipoBarraEstado()
   Dim t
   t = ""
   On Error Resume Next
   Err.Clear
   t = session.findById("wnd[0]/sbar").MessageType
   If Err.Number <> 0 Then
      Err.Clear
      t = session.findById("wnd[0]/sbar/pane[0]").MessageType
   End If
   Err.Clear
   On Error GoTo 0
   ObtenerTipoBarraEstado = UCase(Trim(t))
End Function

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

Sub SaveLatestExcelAs(fullPathXlsx)
   Dim xlApp, wb
   Set xlApp = WaitForExcelApp(60)
   If xlApp Is Nothing Then
      WScript.Echo "ERROR: No se detecto Excel despues de 'Copy to XXL'. Guarde el libro manualmente."
      Exit Sub
   End If
   xlApp.DisplayAlerts = False
   ' Visible = False para no mostrar ventana durante guardado; si Excel abre "como invitado", suele ser porque el script se ejecuta desde otro contexto (ej. servidor web). Ejecutar la descarga con el usuario logueado en la misma PC ayuda.
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

' --- FASE 3: Reutilizar sesion P01 o abrir la entrada LCS ---
Set connection = Nothing
Set session = Nothing
If BuscarSesionP01() Then
   Log "Reutilizando sesion abierta de " & SAP_SYSTEM & " (" & connection.Description & ")."
Else
   Log "No hay sesion de " & SAP_SYSTEM & " abierta. Abriendo " & SAP_CONNECTION_NAME & "..."
   ' En SAP Logon la entrada puede tener dos espacios antes de "LCS"; se prueban ambas formas.
   nombresEntrada = Array(SAP_CONNECTION_NAME, Replace(SAP_CONNECTION_NAME, "Production LCS", "Production  LCS"))
   For Each nombreEntrada In nombresEntrada
      On Error Resume Next
      Set connection = Nothing
      Set connection = application.OpenConnection(nombreEntrada, True)
      errConn = Err.Number
      errConnDesc = Err.Description
      Err.Clear
      On Error GoTo 0
      If errConn = 0 And Not (connection Is Nothing) Then Exit For
   Next
   If errConn <> 0 Or connection Is Nothing Then
      WScript.Echo "ERROR: No se pudo abrir la entrada """ & SAP_CONNECTION_NAME & """ en SAP Logon. " & errConnDesc
      Salir 1
   End If
   maxEsperaSesion = 60
   For esperaSesion = 1 To maxEsperaSesion
      If connection.Children.Count > 0 Then Exit For
      Esperar 1
   Next

   ' --- FASE 4: Sesion ---
   If connection.Children.Count = 0 Then
      WScript.Echo "ERROR: SAP no creo la sesion para " & SAP_CONNECTION_NAME & "."
      Salir 1
   End If
   Set session = connection.Children(0)
End If

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

IniciarSesionSAP
If EsPantallaLoginSAP() Or Len(InfoSesion(session, "User")) = 0 Then
   WScript.Echo "ERROR: No se completo el inicio de sesion en " & SAP_SYSTEM & " / " & SAP_CLIENT & "."
   Salir 1
End If

If InfoSesion(session, "SystemName") <> SAP_SYSTEM Or InfoSesion(session, "Client") <> SAP_CLIENT Then
   WScript.Echo "ERROR: La sesion SAP no es " & SAP_SYSTEM & " / " & SAP_CLIENT & " (SID=" & InfoSesion(session, "SystemName") & ", cliente=" & InfoSesion(session, "Client") & ")."
   Salir 1
End If
Log "Conectado a " & SAP_SYSTEM & " / cliente " & SAP_CLIENT & "."

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

' Ejecutar transaccion KE30 (ventas) - /n + codigo + Enter
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
session.findById("wnd[0]/usr/lbl[1,3]").caretPosition = 15
session.findById("wnd[0]").sendVKey 2
session.findById("wnd[0]/tbar[0]/btn[3]").press

' --- Exportar a Excel: Copy to XXL (no descarga .DAT que sacaba del reporte) ---
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

' Guardar el libro que SAP abrio en Excel como .xlsx
Esperar 2
DeleteIfExists outFile
SaveLatestExcelAs outFile

' --- Cerrar SAP GUI completamente (session + application), igual que exportar-sap.vbs ---
Sub CerrarSAPCompleto()
   Dim wIdx3, wndP
   On Error Resume Next
   If session Is Nothing Then
      Log "  No hay sesion SAP para cerrar."
      On Error GoTo 0
      Exit Sub
   End If
   ' Cerrar popups abiertos primero
   For wIdx3 = 5 To 1 Step -1
      Err.Clear
      Set wndP = Nothing
      Set wndP = session.findById("wnd[" & wIdx3 & "]")
      If Err.Number = 0 And Not (wndP Is Nothing) Then
         session.findById("wnd[" & wIdx3 & "]").close
         Esperar 1
      End If
      Err.Clear
   Next
   ' Cerrar sesion SAP con /nex (cierra sin guardar ni preguntar)
   Log "  Cerrando sesion SAP con /nex..."
   Err.Clear
   session.findById("wnd[0]/tbar[0]/okcd").text = "/nex"
   session.findById("wnd[0]").sendVKey 0
   Esperar 2
   ' Si pide confirmacion, aceptar
   Err.Clear
   Set wndP = Nothing
   Set wndP = session.findById("wnd[1]")
   If Err.Number = 0 And Not (wndP Is Nothing) Then
      session.findById("wnd[1]").sendVKey 0
      Esperar 1
   End If
   Err.Clear
   Log "  Sesion SAP cerrada."
   ' Cerrar SAP GUI completamente
   Log "  Cerrando SAP GUI (application)..."
   Err.Clear
   If Not (connection Is Nothing) Then
      connection.CloseSession session.Id
      Esperar 1
   End If
   Err.Clear
   If Not (connection Is Nothing) Then
      If connection.Children.Count = 0 Then
         connection.CloseConnection
         Esperar 1
      End If
   End If
   Err.Clear
   If Not (application Is Nothing) Then
      If application.Children.Count = 0 Then
         Log "  No quedan conexiones, cerrando SAP Logon..."
         shell.Run "taskkill /F /IM saplogon.exe", 0, True
         Esperar 1
      End If
   End If
   Err.Clear
   Log "  SAP GUI cerrado completamente."
   Set session = Nothing
   Set connection = Nothing
   Set application = Nothing
   On Error GoTo 0
End Sub
CerrarSAPCompleto
