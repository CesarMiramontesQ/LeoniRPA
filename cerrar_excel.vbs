' Script standalone: cierra todas las instancias de Excel (libros y aplicación).
' Se ejecuta desde el backend después de subir los datos para asegurar que Excel se cierre.
Option Explicit

Sub Esperar(segundos)
   WScript.Sleep segundos * 1000
End Sub

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

CerrarExcelesAbiertos
