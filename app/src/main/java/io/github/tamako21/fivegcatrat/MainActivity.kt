package io.github.tamako21.fivegcatrat

import android.Manifest
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.content.SharedPreferences
import android.content.pm.PackageManager
import android.graphics.Color
import android.graphics.drawable.GradientDrawable
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.provider.Settings
import android.view.View
import android.widget.AdapterView
import android.widget.ArrayAdapter
import android.widget.Button
import android.widget.LinearLayout
import android.widget.RadioGroup
import android.widget.SeekBar
import android.widget.Spinner
import android.widget.Switch
import android.widget.TextView
import android.widget.Toast
import androidx.appcompat.app.AlertDialog
import androidx.appcompat.app.AppCompatActivity
import androidx.core.app.ActivityCompat
import org.json.JSONArray
import rikka.shizuku.Shizuku

class MainActivity : AppCompatActivity() {

    companion object {
        private const val UNAVAILABLE_VALUE = Int.MAX_VALUE // 取得不可な値を示す定数
    }

    // エラー内容をキャッシュディレクトリのテキストファイルに書き出す処理
    private fun logErrorToFile(e: Exception, contextMsg: String) {
        try {
            e.printStackTrace()
            val logFile = java.io.File(cacheDir, "debug_log.txt")
            val timeStr = java.text.SimpleDateFormat("yyyy/MM/dd HH:mm:ss", java.util.Locale.getDefault()).format(java.util.Date())
            val errorMsg = "[$timeStr] エラー ($contextMsg): ${e.localizedMessage}\n"
            logFile.appendText(errorMsg)
        } catch (ignored: Exception) {}
    }

    private var isRunning = false
    private var isInitialized = false
    private lateinit var prefs: SharedPreferences
    private var latestHistoryJson = "[]"

    // フォルダ選択用のランチャーと、パスを表示するテキストボックスの一時保存先
    private lateinit var dirPickerLauncher: androidx.activity.result.ActivityResultLauncher<Intent>
    private var currentDialogEditCsvPath: android.widget.EditText? = null

    // サービス（裏側で動く通信監視機能）からのUI更新データを受け取るためのレシーバー
    private val uiUpdateReceiver = object : BroadcastReceiver() {
        override fun onReceive(context: Context?, intent: Intent?) {
            if (intent?.action == "UPDATE_UI_DATA") {
                val summary = intent.getStringExtra("summary") ?: ""
                val jsonStr = intent.getStringExtra("cells_json") ?: "[]"
                val statsJson = intent.getStringExtra("stats_json") ?: "[]"
                latestHistoryJson = intent.getStringExtra("history_json") ?: "[]"

                // 各タブのUI更新メソッドを呼び出し
                updateUiCards(summary, jsonStr)
                updateGraph(jsonStr)
                updateStats(statsJson)
            }
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)

        // フォルダ選択画面(SAF)から戻ってきた時の処理を登録
        dirPickerLauncher = registerForActivityResult(androidx.activity.result.contract.ActivityResultContracts.StartActivityForResult()) { result ->
            if (result.resultCode == RESULT_OK) {
                val uri = result.data?.data
                if (uri != null) {
                    // 端末再起動後もフォルダにアクセスし続けるための権限をOSから取得
                    val takeFlags = Intent.FLAG_GRANT_READ_URI_PERMISSION or Intent.FLAG_GRANT_WRITE_URI_PERMISSION
                    contentResolver.takePersistableUriPermission(uri, takeFlags)

                    // 取得したURI（暗号化されたパス）を入力欄に表示
                    currentDialogEditCsvPath?.setText(uri.toString())
                    Toast.makeText(this, "📁 保存先フォルダを選択しました", Toast.LENGTH_SHORT).show()
                }
            }
        }

        // --- UI部品の取得 ---
        val btnToggle = findViewById<Button>(R.id.btnToggle)
        val textStatus = findViewById<TextView>(R.id.textStatus)
        val spinnerInterval = findViewById<Spinner>(R.id.spinnerInterval)

        val btnForceUpdate = findViewById<Button>(R.id.btnForceUpdate)
        val btnHistory = findViewById<Button>(R.id.btnHistory)
        val btnCopyLog = findViewById<Button>(R.id.btnCopyLog)
        val btnSaveDump = findViewById<Button>(R.id.btnSaveDump)
        val btnAdvancedSettings = findViewById<Button>(R.id.btnAdvancedSettings)
        val btnResetStats = findViewById<Button>(R.id.btnResetStats)

        val tabGroup = findViewById<RadioGroup>(R.id.tabGroup)
        val textTabContent = findViewById<View>(R.id.textTabContent)
        val graphTabContent = findViewById<View>(R.id.graphTabContent)
        val statsTabContent = findViewById<View>(R.id.statsTabContent)

        // タブ切り替えのイベント設定
        tabGroup.setOnCheckedChangeListener { _, checkedId ->
            textTabContent.visibility = View.GONE
            graphTabContent.visibility = View.GONE
            statsTabContent.visibility = View.GONE
            when (checkedId) {
                R.id.tabText -> textTabContent.visibility = View.VISIBLE
                R.id.tabGraph -> graphTabContent.visibility = View.VISIBLE
                R.id.tabStats -> statsTabContent.visibility = View.VISIBLE
            }
        }

        prefs = getSharedPreferences("FiveGCheckerPrefs", Context.MODE_PRIVATE)
        spinnerInterval.setSelection(prefs.getInt("update_interval", 1))

        // 監視中であればボタンのテキストを更新
        isRunning = prefs.getBoolean("is_monitoring", false)
        if (isRunning) {
            btnToggle.text = "監視停止"
            textStatus.text = "状態: 監視中"
        }

        // 更新頻度の変更を監視
        spinnerInterval.post {
            spinnerInterval.onItemSelectedListener = object : AdapterView.OnItemSelectedListener {
                override fun onItemSelected(parent: AdapterView<*>?, view: View?, position: Int, id: Long) {
                    if (!isInitialized) return
                    prefs.edit().putInt("update_interval", position).apply()
                    notifyService() // サービスに変更を通知
                }
                override fun onNothingSelected(parent: AdapterView<*>?) {}
            }
            isInitialized = true
        }

        // 履歴ボタン押下時のダイアログ表示処理
        btnHistory.setOnClickListener {
            val dialogView = layoutInflater.inflate(R.layout.dialog_history, null)
            val container = dialogView.findViewById<LinearLayout>(R.id.historyContainer)

            try {
                val array = JSONArray(latestHistoryJson)
                if (array.length() == 0) {
                    val emptyTv = TextView(this).apply { text = "履歴はまだありません"; setTextColor(Color.GRAY) }
                    container.addView(emptyTv)
                } else {
                    for (i in 0 until array.length()) {
                        val obj = array.getJSONObject(i)
                        val tv = TextView(this).apply {
                            text = "[${obj.getString("time")}] ${obj.getString("msg")}"
                            setTextColor(Color.parseColor(obj.getString("color")))
                            textSize = 13f
                            setPadding(0, 4, 0, 4)
                        }
                        container.addView(tv)
                    }
                }
            } catch (e: Exception) { logErrorToFile(e, "btnHistory") }

            AlertDialog.Builder(this).setView(dialogView).setPositiveButton("閉じる", null).show()
        }

        // 統計・履歴リセットボタンの処理
        btnResetStats.setOnClickListener {
            AlertDialog.Builder(this)
                .setTitle("⚠️ 確認")
                .setMessage("これまでの統計データとタイムライン履歴をすべてリセットしますか？\n（この操作は取り消せません）")
                .setPositiveButton("リセットする") { _, _ ->
                    val intent = Intent(this, NetworkMonitorService::class.java)
                    intent.action = "ACTION_RESET_STATS"
                    startService(intent) // サービスにリセットを指示
                    Toast.makeText(this, "統計と履歴をリセットしました", Toast.LENGTH_SHORT).show()
                }
                .setNegativeButton("キャンセル", null)
                .show()
        }

        // --- 詳細設定ダイアログの構築と表示 ---
        btnAdvancedSettings.setOnClickListener {
            val dialogView = layoutInflater.inflate(R.layout.dialog_settings, null)

            val spinnerSimSlot = dialogView.findViewById<Spinner>(R.id.spinnerSimSlot)
            val simOptions = arrayOf("SIM 1", "SIM 2")
            spinnerSimSlot.adapter = ArrayAdapter(this, android.R.layout.simple_spinner_dropdown_item, simOptions)
            spinnerSimSlot.setSelection(prefs.getInt("pref_sim_slot", 0))

            val spinnerBandDisp = dialogView.findViewById<Spinner>(R.id.spinnerBandDisp)
            val switchShowRsrp = dialogView.findViewById<Switch>(R.id.switchShowRsrp)
            val switchShowRsrq = dialogView.findViewById<Switch>(R.id.switchShowRsrq)
            val switchShowSinr = dialogView.findViewById<Switch>(R.id.switchShowSinr)
            val switchShowCqi = dialogView.findViewById<Switch>(R.id.switchShowCqi)
            val switchShowQl = dialogView.findViewById<Switch>(R.id.switchShowQl)
            val switchShowCong = dialogView.findViewById<Switch>(R.id.switchShowCong)
            val switchShowLevel = dialogView.findViewById<Switch>(R.id.switchShowLevel)
            val switchShowTa = dialogView.findViewById<Switch>(R.id.switchShowTa) // ★追加

            val bandOptions = arrayOf("Bandと周波数の両方 (B1 2.1G)", "Bandのみ (B1)", "周波数のみ (2.1G)")
            spinnerBandDisp.adapter = ArrayAdapter(this, android.R.layout.simple_spinner_dropdown_item, bandOptions)

            // 各種UI設定値の読み込み
            spinnerBandDisp.setSelection(prefs.getInt("pref_show_band", 1))
            switchShowRsrp.isChecked = prefs.getBoolean("pref_show_rsrp", true)
            switchShowRsrq.isChecked = prefs.getBoolean("pref_show_rsrq", false)
            switchShowSinr.isChecked = prefs.getBoolean("pref_show_sinr", false)
            switchShowCqi.isChecked = prefs.getBoolean("pref_show_cqi", true)
            switchShowQl.isChecked = prefs.getBoolean("pref_show_ql", true)
            switchShowCong.isChecked = prefs.getBoolean("pref_show_congestion", false)
            switchShowLevel.isChecked = prefs.getBoolean("pref_show_level", false)
            switchShowTa.isChecked = prefs.getBoolean("pref_show_ta", true)

            val spinnerModeDialog = dialogView.findViewById<Spinner>(R.id.spinnerModeDialog)
            val spinnerValueFormatDialog = dialogView.findViewById<Spinner>(R.id.spinnerValueFormatDialog)
            val spinnerPacketStallDialog = dialogView.findViewById<Spinner>(R.id.spinnerPacketStallDialog)
            val spinnerNcellDialog = dialogView.findViewById<Spinner>(R.id.spinnerNcellDialog)
            val switchStrictStandbyDialog = dialogView.findViewById<Switch>(R.id.switchStrictStandbyDialog)

            spinnerModeDialog.setSelection(prefs.getInt("display_mode", 1))
            spinnerValueFormatDialog.setSelection(prefs.getInt("value_format", 0))
            spinnerPacketStallDialog.setSelection(prefs.getInt("packet_stall_alert", 1))
            spinnerNcellDialog.setSelection(prefs.getInt("ncell_display", 0))
            switchStrictStandbyDialog.isChecked = prefs.getBoolean("strict_standby", false)

            switchStrictStandbyDialog.setOnCheckedChangeListener { _, isChecked ->
                prefs.edit().putBoolean("strict_standby", isChecked).apply()
                notifyService()
            }

            val spinnerTxPowerDialog = dialogView.findViewById<Spinner>(R.id.spinnerTxPowerDialog)
            val spinnerEnvDialog = dialogView.findViewById<Spinner>(R.id.spinnerEnvDialog)

            spinnerTxPowerDialog.setSelection(prefs.getInt("tx_power", 1))
            spinnerEnvDialog.setSelection(prefs.getInt("env_mode", 1))

            val switchCsvLogDialog = dialogView.findViewById<Switch>(R.id.switchCsvLogDialog)
            val switchCsvLogAllCells = dialogView.findViewById<Switch>(R.id.switchCsvLogAllCells)
            val editCsvCustomPath = dialogView.findViewById<android.widget.EditText>(R.id.editCsvCustomPath)

            // 入力欄をタップでフォルダ選択画面が開くようにする (SAF対応)
            currentDialogEditCsvPath = editCsvCustomPath
            editCsvCustomPath.isFocusable = false // キーボード入力を禁止
            editCsvCustomPath.isClickable = true

            // パスが長い場合に折り返して全貌を見えるようにする
            editCsvCustomPath.isSingleLine = false
            editCsvCustomPath.maxLines = 4       // 最大4行まで表示
            editCsvCustomPath.textSize = 13f     // 長いパスを見やすくするため少し文字を小さくする

            editCsvCustomPath.hint = "タップして保存フォルダを選択 (長押しでリセット)"

            // タップ時の処理（Android標準のフォルダ選択を開く）
            editCsvCustomPath.setOnClickListener {
                val intent = Intent(Intent.ACTION_OPEN_DOCUMENT_TREE)
                dirPickerLauncher.launch(intent)
            }
            // 長押し時の処理（パスを空にしてデフォルトのDownloadsに戻す）
            editCsvCustomPath.setOnLongClickListener {
                editCsvCustomPath.setText("")
                Toast.makeText(this, "保存先をデフォルトに戻しました", Toast.LENGTH_SHORT).show()
                true
            }

            switchCsvLogDialog.isChecked = prefs.getBoolean("csv_logging_enabled", false)
            switchCsvLogAllCells.isChecked = prefs.getBoolean("csv_log_all_cells", false)
            val savedPath = prefs.getString("csv_save_path", "")
            editCsvCustomPath.setText(if (savedPath == "") "" else savedPath)

            switchCsvLogAllCells.isEnabled = switchCsvLogDialog.isChecked
            switchCsvLogDialog.setOnCheckedChangeListener { _, isChecked ->
                switchCsvLogAllCells.isEnabled = isChecked
            }

            val switchOverlayDialog = dialogView.findViewById<Switch>(R.id.switchOverlayDialog)
            val spinnerOverlayGravityDialog = dialogView.findViewById<Spinner>(R.id.spinnerOverlayGravityDialog)
            val seekOverlaySizeDialog = dialogView.findViewById<SeekBar>(R.id.seekOverlaySizeDialog)
            val seekOverlayXDialog = dialogView.findViewById<SeekBar>(R.id.seekOverlayXDialog)
            val seekOverlayYDialog = dialogView.findViewById<SeekBar>(R.id.seekOverlayYDialog)

            switchOverlayDialog.isChecked = prefs.getBoolean("overlay_enabled", false)
            spinnerOverlayGravityDialog.setSelection(prefs.getInt("overlay_gravity", 0))
            seekOverlaySizeDialog.progress = prefs.getInt("overlay_size", 12)
            seekOverlayXDialog.progress = prefs.getInt("overlay_x", 500)
            seekOverlayYDialog.progress = prefs.getInt("overlay_y", 0)

            // オーバーレイ表示のトグルイベント（権限チェックを含む）
            switchOverlayDialog.setOnCheckedChangeListener { _, isChecked ->
                if (isChecked && !Settings.canDrawOverlays(this)) {
                    Toast.makeText(this, "「他のアプリの上に表示」を許可してください", Toast.LENGTH_SHORT).show()
                    val intent = Intent(Settings.ACTION_MANAGE_OVERLAY_PERMISSION, Uri.parse("package:$packageName"))
                    startActivity(intent)
                    switchOverlayDialog.isChecked = false
                    return@setOnCheckedChangeListener
                }
                prefs.edit().putBoolean("overlay_enabled", isChecked).apply()
                notifyService()
            }

            val seekListener = object : SeekBar.OnSeekBarChangeListener {
                override fun onProgressChanged(seekBar: SeekBar?, progress: Int, fromUser: Boolean) {
                    if (fromUser) {
                        when (seekBar?.id) {
                            R.id.seekOverlaySizeDialog -> prefs.edit().putInt("overlay_size", progress).apply()
                            R.id.seekOverlayXDialog -> prefs.edit().putInt("overlay_x", progress).apply()
                            R.id.seekOverlayYDialog -> prefs.edit().putInt("overlay_y", progress).apply()
                        }
                        notifyService()
                    }
                }
                override fun onStartTrackingTouch(seekBar: SeekBar?) {}
                override fun onStopTrackingTouch(seekBar: SeekBar?) {}
            }
            seekOverlaySizeDialog.setOnSeekBarChangeListener(seekListener)
            seekOverlayXDialog.setOnSeekBarChangeListener(seekListener)
            seekOverlayYDialog.setOnSeekBarChangeListener(seekListener)

            val spinnerPingFreq = dialogView.findViewById<Spinner>(R.id.spinnerPingFreq)
            val spinnerPingTarget = dialogView.findViewById<Spinner>(R.id.spinnerPingTarget)
            val editPingCustomIp = dialogView.findViewById<android.widget.EditText>(R.id.editPingCustomIp)
            val spinnerPingDisplay = dialogView.findViewById<Spinner>(R.id.spinnerPingDisplay)
            val spinnerPingWifi = dialogView.findViewById<Spinner>(R.id.spinnerPingWifi)

            spinnerPingFreq.setSelection(prefs.getInt("ping_freq", 0))
            spinnerPingTarget.setSelection(prefs.getInt("ping_target", 0))
            editPingCustomIp.setText(prefs.getString("ping_custom_ip", ""))
            spinnerPingDisplay.setSelection(if (prefs.getBoolean("ping_display", true)) 1 else 0)
            spinnerPingWifi.setSelection(if (prefs.getBoolean("ping_on_wifi", false)) 1 else 0)

            // ダイアログ内の各Spinnerの設定を保存・反映するリスナー
            val dialogItemSelectedListener = object : AdapterView.OnItemSelectedListener {
                override fun onItemSelected(parent: AdapterView<*>?, view: View?, position: Int, id: Long) {
                    val editor = prefs.edit()
                    when(parent?.id) {
                        R.id.spinnerSimSlot -> editor.putInt("pref_sim_slot", position)
                        R.id.spinnerModeDialog -> editor.putInt("display_mode", position)
                        R.id.spinnerValueFormatDialog -> editor.putInt("value_format", position)
                        R.id.spinnerTxPowerDialog -> editor.putInt("tx_power", position)
                        R.id.spinnerEnvDialog -> editor.putInt("env_mode", position)
                        R.id.spinnerPacketStallDialog -> editor.putInt("packet_stall_alert", position)
                        R.id.spinnerNcellDialog -> editor.putInt("ncell_display", position)
                        R.id.spinnerOverlayGravityDialog -> editor.putInt("overlay_gravity", position)
                        R.id.spinnerPingFreq -> editor.putInt("ping_freq", position)
                        R.id.spinnerPingTarget -> editor.putInt("ping_target", position)
                        R.id.spinnerPingDisplay -> editor.putBoolean("ping_display", position == 1)
                        R.id.spinnerPingWifi -> editor.putBoolean("ping_on_wifi", position == 1)
                    }
                    editor.apply()
                }
                override fun onNothingSelected(parent: AdapterView<*>?) {}
            }

            spinnerSimSlot.onItemSelectedListener = dialogItemSelectedListener
            spinnerModeDialog.onItemSelectedListener = dialogItemSelectedListener
            spinnerValueFormatDialog.onItemSelectedListener = dialogItemSelectedListener
            spinnerTxPowerDialog.onItemSelectedListener = dialogItemSelectedListener
            spinnerEnvDialog.onItemSelectedListener = dialogItemSelectedListener
            spinnerPacketStallDialog.onItemSelectedListener = dialogItemSelectedListener
            spinnerNcellDialog.onItemSelectedListener = dialogItemSelectedListener
            spinnerOverlayGravityDialog.onItemSelectedListener = dialogItemSelectedListener
            spinnerPingFreq.onItemSelectedListener = dialogItemSelectedListener
            spinnerPingTarget.onItemSelectedListener = dialogItemSelectedListener
            spinnerPingDisplay.onItemSelectedListener = dialogItemSelectedListener
            spinnerPingWifi.onItemSelectedListener = dialogItemSelectedListener

            AlertDialog.Builder(this)
                .setTitle("⚙️ 詳細設定")
                .setView(dialogView)
                .setPositiveButton("保存して閉じる") { _, _ ->
                    // スイッチや入力欄の状態を保存
                    prefs.edit()
                        .putInt("pref_show_band", spinnerBandDisp.selectedItemPosition)
                        .putBoolean("pref_show_rsrp", switchShowRsrp.isChecked)
                        .putBoolean("pref_show_rsrq", switchShowRsrq.isChecked)
                        .putBoolean("pref_show_sinr", switchShowSinr.isChecked)
                        .putBoolean("pref_show_cqi", switchShowCqi.isChecked)
                        .putBoolean("pref_show_ql", switchShowQl.isChecked)
                        .putBoolean("pref_show_congestion", switchShowCong.isChecked)
                        .putBoolean("pref_show_level", switchShowLevel.isChecked)
                        .putBoolean("pref_show_ta", switchShowTa.isChecked)
                        .putString("ping_custom_ip", editPingCustomIp.text.toString().trim())
                        .putBoolean("csv_logging_enabled", switchCsvLogDialog.isChecked)
                        .putBoolean("csv_log_all_cells", switchCsvLogAllCells.isChecked)
                        .putString("csv_save_path", editCsvCustomPath.text.toString().trim())
                        .apply()
                    notifyService(true) // 変更をサービスに伝える
                    Toast.makeText(this, "設定を適用しました", Toast.LENGTH_SHORT).show()
                }
                .setNegativeButton("キャンセル", null)
                .show()
        }

        // Shizukuのパーミッション要求の結果を受け取るコールバック
        Shizuku.addRequestPermissionResultListener { requestCode, grantResult ->
            if (requestCode == 1 && grantResult == PackageManager.PERMISSION_GRANTED) {
                checkPermissionsAndStart(btnToggle, textStatus)
            } else {
                Toast.makeText(this, "Shizukuの権限が必要です", Toast.LENGTH_SHORT).show()
            }
        }

        // 監視開始・停止のトグルボタン処理
        btnToggle.setOnClickListener {
            if (isRunning) {
                stopService(Intent(this, NetworkMonitorService::class.java))
                isRunning = false
                prefs.edit().putBoolean("is_monitoring", false).apply()
                btnToggle.text = "監視開始"
                textStatus.text = "状態: 停止中"
            } else {
                if (Shizuku.checkSelfPermission() == PackageManager.PERMISSION_GRANTED) {
                    checkPermissionsAndStart(btnToggle, textStatus)
                } else if (Shizuku.pingBinder()) {
                    Shizuku.requestPermission(1)
                } else {
                    Toast.makeText(this, "Shizukuが起動していません", Toast.LENGTH_SHORT).show()
                }
            }
        }

        btnForceUpdate.setOnClickListener { notifyService(true) }

        btnCopyLog.setOnClickListener {
            try {
                val logFile = java.io.File(cacheDir, "debug_log.txt")
                if (logFile.exists()) {
                    val logText = logFile.readText()
                    val clipboard = getSystemService(Context.CLIPBOARD_SERVICE) as android.content.ClipboardManager
                    clipboard.setPrimaryClip(android.content.ClipData.newPlainText("5G CAT-RAT Log", logText))
                    Toast.makeText(this, "📋 ログをコピーしました！", Toast.LENGTH_SHORT).show()
                } else {
                    Toast.makeText(this, "⚠️ まだログがありません", Toast.LENGTH_SHORT).show()
                }
            } catch (e: Exception) { e.printStackTrace() }
        }

        btnSaveDump.setOnClickListener { saveRawDumpToFile() }

        // Androidバージョンに応じたレシーバーの登録（TIRAMISU以降は明示的なエクスポート設定が必要）
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            registerReceiver(uiUpdateReceiver, IntentFilter("UPDATE_UI_DATA"), Context.RECEIVER_NOT_EXPORTED)
        } else {
            registerReceiver(uiUpdateReceiver, IntentFilter("UPDATE_UI_DATA"))
        }
    }

    // サービスに対して意図的なアップデート要求を送る
    private fun notifyService(isManual: Boolean = false) {
        if (isRunning) {
            val intent = Intent(this, NetworkMonitorService::class.java)
            intent.action = if (isManual) "ACTION_MANUAL_UPDATE" else "UPDATE_PREFS"
            startService(intent)
        }
    }

    // 各種権限が許可されているか確認し、不足していればリクエストする
    private fun checkPermissionsAndStart(btnToggle: Button, textStatus: TextView) {
        val requiredPerms = mutableListOf(Manifest.permission.READ_PHONE_STATE, Manifest.permission.ACCESS_FINE_LOCATION)
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            requiredPerms.add(Manifest.permission.POST_NOTIFICATIONS)
        }

        val missingPerms = requiredPerms.filter { ActivityCompat.checkSelfPermission(this, it) != PackageManager.PERMISSION_GRANTED }

        if (missingPerms.isNotEmpty()) {
            ActivityCompat.requestPermissions(this, missingPerms.toTypedArray(), 100)
            return
        }

        startService(Intent(this, NetworkMonitorService::class.java))
        isRunning = true
        prefs.edit().putBoolean("is_monitoring", true).apply()
        btnToggle.text = "監視停止"
        textStatus.text = "状態: 監視中"
    }

    override fun onDestroy() {
        super.onDestroy()
        Shizuku.removeRequestPermissionResultListener { _, _ -> }
        try { unregisterReceiver(uiUpdateReceiver) } catch (e: Exception) {}
    }

    // dumpsysの生データ（プレーンテキスト）をDownloadsフォルダに保存する処理
    private fun saveRawDumpToFile() {
        try {
            val rawFile = java.io.File(cacheDir, "raw_dumpsys.txt")
            if (!rawFile.exists()) {
                Toast.makeText(this, "⚠️ まだ生ダンプデータがありません", Toast.LENGTH_SHORT).show()
                return
            }

            val timeStr = java.text.SimpleDateFormat("yyyyMMdd_HHmmss", java.util.Locale.getDefault()).format(java.util.Date())
            val fileName = "5GCATRAT_Dump_$timeStr.txt"
            val rawContent = rawFile.readText()

            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
                // MediaStoreを用いたモダンな保存方法
                val resolver = contentResolver
                val contentValues = android.content.ContentValues().apply {
                    put(android.provider.MediaStore.MediaColumns.DISPLAY_NAME, fileName)
                    put(android.provider.MediaStore.MediaColumns.MIME_TYPE, "text/plain")
                    put(android.provider.MediaStore.MediaColumns.RELATIVE_PATH, android.os.Environment.DIRECTORY_DOWNLOADS + "/5GCATRAT")
                }
                val uri = resolver.insert(android.provider.MediaStore.Downloads.EXTERNAL_CONTENT_URI, contentValues)
                if (uri != null) {
                    resolver.openOutputStream(uri)?.use { outputStream ->
                        outputStream.write(rawContent.toByteArray())
                    }
                    Toast.makeText(this, "💾 Downloads/5GCATRAT/ にダンプを保存しました！", Toast.LENGTH_LONG).show()
                } else {
                    Toast.makeText(this, "⚠️ 保存に失敗しました", Toast.LENGTH_SHORT).show()
                }
            } else {
                // 従来のファイル保存方法
                val downloadDir = java.io.File(android.os.Environment.getExternalStoragePublicDirectory(android.os.Environment.DIRECTORY_DOWNLOADS), "5GCATRAT")
                if (!downloadDir.exists()) downloadDir.mkdirs()
                val outFile = java.io.File(downloadDir, fileName)
                outFile.writeText(rawContent)
                Toast.makeText(this, "💾 Downloads/5GCATRAT/ にダンプを保存しました！", Toast.LENGTH_LONG).show()
            }
        } catch (e: Exception) {
            e.printStackTrace()
            Toast.makeText(this, "⚠️ エラーが発生しました: ${e.localizedMessage}", Toast.LENGTH_SHORT).show()
        }
    }

    // --- 統計タブ（Stats）を構築する処理 ---
    private fun updateStats(statsJson: String) {
        val container = findViewById<LinearLayout>(R.id.statsContainer)
        container.removeAllViews()
        try {
            val array = JSONArray(statsJson)
            if (array.length() == 0) {
                container.addView(TextView(this).apply { text = "データ収集中..."; setTextColor(Color.GRAY) })
                return
            }

            var maxTotal = 0L
            for (i in 0 until array.length()) {
                val total = array.getJSONObject(i).getLong("total")
                if (total > maxTotal) maxTotal = total
            }

            val medals = arrayOf("👑 1位", "🥈 2位", "🥉 3位")

            for (i in 0 until array.length()) {
                val obj = array.getJSONObject(i)
                val band = obj.getString("band")
                val type = obj.getString("type")
                val freq = obj.getString("freq")
                val pTime = obj.getLong("pTime")
                val sTime = obj.getLong("sTime")
                val total = obj.getLong("total")

                val rankStr = if (i < 3) medals[i] else " ${i+1}位"
                val prefix = if (type.contains("5G")) "n" else "B"
                val title = "$rankStr：$prefix$band $freq ($type)"

                val pMin = pTime / 60000
                val pSec = (pTime % 60000) / 1000
                val sMin = sTime / 60000
                val sSec = (sTime % 60000) / 1000
                val timeStr = "[P] ${pMin}分${String.format("%02d", pSec)}秒　[S] ${sMin}分${String.format("%02d", sSec)}秒"

                val itemLayout = LinearLayout(this).apply {
                    orientation = LinearLayout.VERTICAL
                    setPadding(0, 0, 0, 24)
                }

                val tvTitle = TextView(this).apply {
                    text = title
                    setTextColor(Color.WHITE)
                    textSize = 15f
                    setTypeface(null, android.graphics.Typeface.BOLD)
                }
                val tvTime = TextView(this).apply {
                    text = timeStr
                    setTextColor(Color.LTGRAY)
                    textSize = 13f
                    setPadding(16, 2, 0, 8)
                }

                // グラフ（プログレスバー）の背景枠
                val barContainer = android.widget.FrameLayout(this).apply {
                    layoutParams = LinearLayout.LayoutParams(LinearLayout.LayoutParams.MATCH_PARENT, (8 * resources.displayMetrics.density).toInt()).apply {
                        setMargins(16, 0, 16, 0)
                    }
                    background = GradientDrawable().apply { setColor(Color.parseColor("#333333")); cornerRadius = 8f }
                }

                val fillWidthPercent = if (maxTotal > 0) (total.toFloat() / maxTotal.toFloat()) else 0f
                val barColor = if (type.contains("5G")) "#9C27B0" else "#2196F3"
                val barFill = View(this).apply {
                    layoutParams = android.widget.FrameLayout.LayoutParams(
                        (resources.displayMetrics.widthPixels * fillWidthPercent * 0.85).toInt(),
                        android.widget.FrameLayout.LayoutParams.MATCH_PARENT
                    )
                    background = GradientDrawable().apply { setColor(Color.parseColor(barColor)); cornerRadius = 8f }
                }

                barContainer.addView(barFill)
                itemLayout.addView(tvTitle)
                itemLayout.addView(tvTime)
                itemLayout.addView(barContainer)

                container.addView(itemLayout)
            }
        } catch (e: Exception) {
            logErrorToFile(e, "updateStats")
        }
    }

    // 周波数帯（Band）に対応する一般的な名称を返すメソッド
    private fun getFrequencyName(band: String, isNr: Boolean): String {
        val cleanBand = band.replace(Regex("[^0-9]"), "")
        return if (isNr) {
            when (cleanBand) {
                "3" -> "1.8G"; "28" -> "700M"; "41" -> "2.5G"; "77" -> "3.7/4.0G"
                "78" -> "3.5/3.7G"; "79" -> "4.5G"; "257" -> "28G"; else -> ""
            }
        } else {
            when (cleanBand) {
                "1" -> "2.1G"; "3" -> "1.8G"; "8" -> "900M"; "11", "21" -> "1.5G"
                "18", "19", "26" -> "800M"; "28" -> "700M"; "41" -> "2.5G"; "42" -> "3.5G"
                else -> ""
            }
        }
    }

    // --- テキストタブ（カード表示）を構築する処理 ---
    private fun updateUiCards(summary: String, jsonStr: String) {
        val tvSummary = findViewById<TextView>(R.id.tvSummary)
        val cardContainer = findViewById<LinearLayout>(R.id.cardContainer)
        tvSummary.text = summary
        cardContainer.removeAllViews()

        val prefBandDisp = prefs.getInt("pref_show_band", 0)
        val showCongestion = prefs.getBoolean("pref_show_congestion", false)
        val showQl = prefs.getBoolean("pref_show_ql", true)
        val showLevel = prefs.getBoolean("pref_show_level", false)

        try {
            val array = JSONArray(jsonStr)
            for (i in 0 until array.length()) {
                val obj = array.getJSONObject(i)
                val type = obj.getString("type")
                val role = obj.getString("role")
                val band = obj.getString("band")
                val bw = obj.getInt("bw")
                val arfcn = obj.getInt("arfcn")
                val pci = obj.getInt("pci")
                val rsrp = obj.getInt("rsrp")
                val rsrq = obj.getInt("rsrq")
                val rsrpLv = obj.getInt("rsrpLv")
                val rsrqLv = obj.getInt("rsrqLv")
                val ql = obj.getInt("ql")
                val distance = obj.getString("distance")
                val realFreq = obj.getString("realFreq")
                val expectedRsrq = obj.getInt("expectedRsrq")
                val congestionText = obj.getString("congestionText")
                val cqi = obj.optInt("cqi", UNAVAILABLE_VALUE)
                val sinr = obj.optInt("sinr", UNAVAILABLE_VALUE)
                val carrier = obj.optString("carrier", "")
                val isEmergency = obj.optBoolean("isEmergency", false)

                val ci = obj.optLong("ci", -1L)
                val enbStr = if (ci > 0L && ci != 2147483647L && ci != Long.MAX_VALUE) {
                    if (type == "5G") {
                        val gnb = ci / 4096L
                        "  (gNB: $gnb)"
                    } else {
                        val enb = ci / 256L
                        "  (eNB: $enb)"
                    }
                } else ""

                val isInvalid = rsrp == UNAVAILABLE_VALUE
                val isNCell = role == "N"

                // 背景色と枠線の色を計算（PCell/SCell、圏外などで変動）
                val bgColor = if (isNCell) Color.parseColor("#1A1A1A") else Color.parseColor("#000000")
                val borderColor = when {
                    isNCell || isInvalid -> Color.parseColor("#444444")
                    ql >= 7 -> Color.parseColor("#4CAF50")
                    ql >= 4 -> Color.parseColor("#FFC107")
                    else -> Color.parseColor("#F44336")
                }

                // 品質に応じた枠線の太さと角丸設定
                val strokeW = (if (isInvalid || isNCell) 2 else (ql * 0.8 + 2).toInt()) * resources.displayMetrics.density
                val shape = GradientDrawable().apply { shape = GradientDrawable.RECTANGLE; setColor(bgColor); setStroke(strokeW.toInt(), borderColor); cornerRadius = 24f }

                // 実際のカード用レイアウト
                val cardLayout = LinearLayout(this).apply {
                    orientation = LinearLayout.VERTICAL; background = shape; setPadding(32, 24, 32, 24)
                    layoutParams = LinearLayout.LayoutParams(LinearLayout.LayoutParams.MATCH_PARENT, LinearLayout.LayoutParams.WRAP_CONTENT).apply { setMargins(0, 0, 0, 16) }
                }

                val pciStr = if (pci > 0) pci.toString() else "不明"
                val carrierStr = if (carrier.isNotEmpty()) " $carrier" else ""
                val prefix = if (type.contains("5G")) "n" else "B"
                val freqName = getFrequencyName(band, type == "5G")

                val bandName = when (prefBandDisp) {
                    1 -> "$prefix$band"
                    2 -> freqName.ifEmpty { "$prefix$band" }
                    else -> "$prefix$band" + (if (freqName.isNotEmpty()) " $freqName" else "")
                }

                val titleStr = if (isEmergency) {
                    "🚨 緊急通報専用 [$role] $bandName / PCI: $pciStr$enbStr$carrierStr"
                } else if (isNCell) {
                    "$type[$role] $bandName / PCI: $pciStr$enbStr$carrierStr"
                } else {
                    "$type[$role] $bandName (${if (bw>0) "${bw}MHz" else "不明"})  /  PCI: $pciStr$enbStr$carrierStr"
                }

                val freqStr = "中心周波数: $realFreq (ARFCN: $arfcn)"

                // カード表示を「測定不能」から「-」へ
                val lvRsrpSuffix = if (showLevel) " (Lv.$rsrpLv)" else ""
                val rsrpStr = if (isInvalid) "-" else "${rsrp}dBm$lvRsrpSuffix"

                val lvRsrqSuffix = if (showLevel) " (Lv.$rsrqLv)" else ""
                val rsrqStr = if (rsrq == UNAVAILABLE_VALUE) "-" else "${rsrq}dB$lvRsrqSuffix"

                val sinrStr = if (sinr == UNAVAILABLE_VALUE) "-" else sinr.toString()
                val cqiStr = if (cqi == UNAVAILABLE_VALUE || cqi == 0) "-" else cqi.toString()

                val detailStr = if (isNCell) {
                    val nCellList = mutableListOf<String>()
                    nCellList.add("RSRP: $rsrpStr")
                    nCellList.add("RSRQ: $rsrqStr")
                    nCellList.add("SINR: $sinrStr") // ★固定表示化
                    nCellList.add("CQI: $cqiStr")   // ★固定表示化

                    nCellList.joinToString("  |  ")
                } else {
                    val expectedRsrqStr = if (expectedRsrq == UNAVAILABLE_VALUE) "-" else "${expectedRsrq}dB"
                    val rsrqFinal = if (showCongestion && expectedRsrq != UNAVAILABLE_VALUE && rsrq != UNAVAILABLE_VALUE) {
                        "$rsrqStr [期待値 $expectedRsrqStr]"
                    } else {
                        rsrqStr
                    }
                    val line1 = "RSRP: $rsrpStr  |  RSRQ: $rsrqFinal"

                    val qualityList = mutableListOf<String>()
                    qualityList.add("SINR: $sinrStr") // ★固定表示化
                    qualityList.add("CQI: $cqiStr")   // ★固定表示化
                    qualityList.add(distance)         // ★固定表示化
                    if (showQl) qualityList.add("品質: QL $ql") // ★設定反映
                    if (showCongestion) qualityList.add("混雑度: $congestionText") // ★設定反映

                    val line2 = qualityList.joinToString("  |  ")

                    "$line1\n$line2"
                }

                val tvTitle = TextView(this).apply { text = titleStr; textSize = 16f; setTypeface(null, android.graphics.Typeface.BOLD); setTextColor(if(isNCell) Color.parseColor("#AAAAAA") else Color.parseColor("#FFFFFF")) }
                val tvFreq = TextView(this).apply { text = freqStr; textSize = 14f; setTextColor(Color.parseColor("#888888")); setPadding(0,4,0,8) }
                val tvDetail = TextView(this).apply { text = detailStr; textSize = 14f; setTextColor(if(isNCell) Color.parseColor("#AAAAAA") else Color.parseColor("#EEEEEE")) }

                cardLayout.addView(tvTitle); cardLayout.addView(tvFreq); cardLayout.addView(tvDetail)
                cardContainer.addView(cardLayout)
            }
        } catch (e: Exception) {
            logErrorToFile(e, "updateUiCards")
        }
    }

    // --- グラフタブ（PipeGraphView）を構築する処理 ---
    private fun updateGraph(jsonStr: String) {
        val pipeGraphView = findViewById<PipeGraphView>(R.id.mainPipeGraphView)
        val valFormat = prefs.getInt("value_format", 0)

        val prefBandDisp = prefs.getInt("pref_show_band", 0)
        val showRsrp = prefs.getBoolean("pref_show_rsrp", true)
        val showRsrq = prefs.getBoolean("pref_show_rsrq", true)
        val showSinr = prefs.getBoolean("pref_show_sinr", false)
        val showCqi = prefs.getBoolean("pref_show_cqi", false)
        val showCongestion = prefs.getBoolean("pref_show_congestion", false)
        val showQl = prefs.getBoolean("pref_show_ql", true)
        val showTa = prefs.getBoolean("pref_show_ta", true)

        try {
            val array = JSONArray(jsonStr)
            val graphDataList = mutableListOf<PipeGraphView.CellGraphData>()
            for (i in 0 until array.length()) {
                val obj = array.getJSONObject(i)
                val role = obj.getString("role")
                if (role == "N") continue // グラフではNCellは描画しない

                val bw = obj.getInt("bw")
                if (bw > 0) {
                    val rawBand = obj.getString("band").replace(Regex("[^0-9]"), "")
                    val type = obj.getString("type")
                    val prefix = if (type.contains("5G")) "n" else "B"
                    val realFreqStr = obj.getString("realFreq").replace(" MHz", "MHz")

                    val bandName = when (prefBandDisp) {
                        1 -> "$prefix$rawBand"
                        2 -> realFreqStr
                        else -> "$prefix$rawBand $realFreqStr"
                    }

                    val rsrp = obj.getInt("rsrp")
                    val rsrq = obj.getInt("rsrq")
                    val sinr = obj.optInt("sinr", UNAVAILABLE_VALUE)
                    val cqi = obj.optInt("cqi", UNAVAILABLE_VALUE)

                    val isInvalid = rsrp == UNAVAILABLE_VALUE

                    val sinrStr = if (sinr == UNAVAILABLE_VALUE) "-" else sinr.toString()
                    val cqiStr = if (cqi == UNAVAILABLE_VALUE || cqi == 0) "-" else cqi.toString()

                    val valStr = if (isInvalid) {
                        "-"
                    } else if (valFormat == 1) {
                        val lvList = mutableListOf<String>()
                        if (showRsrp) lvList.add("Lv.${obj.getInt("rsrpLv")}")
                        if (showRsrq && rsrq != UNAVAILABLE_VALUE) lvList.add("Lv.${obj.getInt("rsrqLv")}")
                        if (showSinr) lvList.add("S:$sinrStr")
                        if (showCqi) lvList.add("C:$cqiStr")
                        lvList.joinToString("/")
                    } else {
                        val valList = mutableListOf<String>()
                        if (showRsrp) valList.add("${rsrp}dBm")
                        if (showRsrq && rsrq != UNAVAILABLE_VALUE) valList.add("${rsrq}dB")
                        if (showSinr) valList.add("S:$sinrStr")
                        if (showCqi) valList.add("C:$cqiStr")
                        valList.joinToString("/")
                    }

                    val congestionSymbol = if (showCongestion) obj.getString("congestionSymbol") else ""
                    val distanceStr = if (showTa) obj.getString("distance") else ""

                    graphDataList.add(PipeGraphView.CellGraphData(
                        type = type,
                        role = role,
                        band = bandName,
                        bwMhz = bw,
                        realFreq = "",
                        valStr = valStr,
                        distance = distanceStr, // ★変更
                        congestionSymbol = congestionSymbol,
                        ql = if (showQl) obj.getInt("ql") else -1
                    ))
                }
            }
            pipeGraphView.setData(graphDataList) // データを渡してグラフを更新
        } catch (e: Exception) {
            logErrorToFile(e, "updateGraph")
        }
    }
}