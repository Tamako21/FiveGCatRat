package io.github.tamako21.fivegcatrat

import android.Manifest
import android.annotation.SuppressLint
import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.content.pm.PackageManager
import android.content.pm.ServiceInfo
import android.graphics.Bitmap
import android.graphics.Canvas
import android.graphics.Color
import android.graphics.Paint
import android.graphics.PixelFormat
import android.graphics.PorterDuff
import android.graphics.Rect
import android.graphics.Typeface
import android.graphics.drawable.Icon
import android.net.ConnectivityManager
import android.net.NetworkCapabilities
import android.os.Build
import android.os.Handler
import android.os.IBinder
import android.os.Looper
import android.provider.Settings
import android.telephony.CellIdentityLte
import android.telephony.CellIdentityNr
import android.telephony.CellInfoLte
import android.telephony.CellInfoNr
import android.telephony.CellSignalStrengthLte
import android.telephony.CellSignalStrengthNr
import android.telephony.SubscriptionManager
import android.telephony.TelephonyCallback
import android.telephony.TelephonyDisplayInfo
import android.telephony.TelephonyManager
import android.view.Gravity
import android.view.WindowManager
import android.widget.TextView
import androidx.core.content.ContextCompat
import rikka.shizuku.Shizuku
import java.io.BufferedReader
import java.io.InputStreamReader
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale
import kotlin.math.min
import kotlin.math.pow

// アプリのバックグラウンド処理（通信監視、通知、データ解析）をすべて統括するメインサービス
class NetworkMonitorService : Service() {

    private val CHANNEL_ID = "NetworkMonitorChannel_v52_Hyphen"
    private val NOTIFICATION_ID = 1

    private lateinit var telephonyManager: TelephonyManager
    private var telephonyCallback: NetworkCallback? = null

    private val handler = Handler(Looper.getMainLooper())
    private var isScreenOn = true

    // 画面に常に最前面表示させるオーバーレイ（小窓）用の変数
    private var windowManager: WindowManager? = null
    private var overlayView: TextView? = null

    // 画面点灯直後など、一時的に通信断の判定を遅らせるためのタイマー
    private var ignoreDisconnectUntil = System.currentTimeMillis() + 5000L

    // Ping（パケ詰まり監視）用の状態保持変数群
    @Volatile private var currentPingMs: Double = -1.0
    @Volatile private var isPingTimeout: Boolean = false
    @Volatile private var isPingExecuting: Boolean = false

    private val pingExecutor = java.util.concurrent.Executors.newSingleThreadExecutor()

    // GPS（位置情報）とCSVログ保存用の変数群
    private var locationManager: android.location.LocationManager? = null
    @Volatile private var currentLat: Double = 0.0
    @Volatile private var currentLon: Double = 0.0
    @Volatile var isCsvLoggingEnabled: Boolean = false
    @Volatile var isCsvLogAllCells: Boolean = false

    // 位置情報が更新されたときに呼ばれるリスナー
    private val locationListener = object : android.location.LocationListener {
        override fun onLocationChanged(location: android.location.Location) {
            currentLat = location.latitude
            currentLon = location.longitude
        }
        override fun onStatusChanged(provider: String?, status: Int, extras: android.os.Bundle?) {}
        override fun onProviderEnabled(provider: String) {}
        override fun onProviderDisabled(provider: String) {}
    }

    // アプリ全体で共有するデータ（統計情報や履歴）を保持するオブジェクト
    companion object {
        const val UNAVAILABLE_VALUE = Int.MAX_VALUE // データが取得できなかった場合の異常値

        val bandStats = mutableMapOf<String, BandStat>() // 各バンドの接続時間を集計するマップ
        val historyList = mutableListOf<HistoryEvent>()  // タイムライン履歴のリスト

        var lastPCellKey = ""
        var lastAlertState = ""
        var wasSaConnected = false
        var wasDisconnected = false
        var lastStatsUpdateTime = 0L
        private var lastActiveChannels = listOf<ChannelData>()

        fun resetStatsAndHistory() {
            bandStats.clear()
            historyList.clear()
            lastPCellKey = ""
            lastAlertState = ""
            wasSaConnected = false
            wasDisconnected = false
            lastStatsUpdateTime = 0L
            lastActiveChannels = emptyList()
        }
    }

    // エラー発生時に内容をテキストファイル（debug_log.txt）に書き出すユーティリティ
    private fun logErrorToFile(e: Exception, contextMsg: String) {
        try {
            e.printStackTrace()
            val logFile = java.io.File(cacheDir, "debug_log.txt")
            val timeStr = java.text.SimpleDateFormat("yyyy/MM/dd HH:mm:ss", java.util.Locale.getDefault()).format(java.util.Date())
            val errorMsg = "[$timeStr] エラー (Service/$contextMsg): ${e.localizedMessage}\n"
            logFile.appendText(errorMsg)
        } catch (ignored: Exception) {}
    }

    // 統計データと履歴データの型定義
    data class BandStat(val type: String, val band: String, val freq: String, var pTimeMs: Long = 0L, var sTimeMs: Long = 0L)
    data class HistoryEvent(val time: String, val msg: String, val colorHex: String)

    // 定期的に通信状態を取得（ポーリング）するための実行タスク
    private val pollingRunnable = object : Runnable {
        override fun run() {
            if (isScreenOn) {
                processNetworkState(force = false) // データ取得開始
                handler.postDelayed(this, getPollingIntervalMs()) // 設定された秒数後に再度実行
            }
        }
    }

    // パケ詰まりを検知するために定期的にPing（疎通確認）を打つタスク
    private val pingRunnable = object : Runnable {
        override fun run() {
            val prefs = getSharedPreferences("FiveGCheckerPrefs", Context.MODE_PRIVATE)
            val freqIdx = prefs.getInt("ping_freq", 0)

            if (freqIdx == 0 || !isScreenOn) {
                handler.postDelayed(this, 5000L)
                return
            }

            val intervalMs = when(freqIdx) { 1->2000L; 2->5000L; 3->10000L; 4->30000L; 5->60000L; else->5000L }

            // Wi-Fi接続時はPing監視を止めるかどうかの判定
            if (isWifiConnected() && !prefs.getBoolean("ping_on_wifi", false)) {
                currentPingMs = -1.0
                isPingTimeout = false
                handler.postDelayed(this, intervalMs)
                return
            }

            // 実際にPingコマンドを実行する非同期処理
            if (!isPingExecuting) {
                isPingExecuting = true
                pingExecutor.execute {
                    val targetIdx = prefs.getInt("ping_target", 0)
                    val ip = when (targetIdx) {
                        0 -> "8.8.8.8"
                        1 -> "1.1.1.1"
                        else -> prefs.getString("ping_custom_ip", "8.8.8.8").takeIf { !it.isNullOrBlank() } ?: "8.8.8.8"
                    }

                    var resultMs = -1.0
                    var timeout = true
                    var process: java.lang.Process? = null
                    try {
                        process = Runtime.getRuntime().exec("ping -c 1 -w 3 $ip")
                        val reader = BufferedReader(InputStreamReader(process.inputStream))
                        var line: String?
                        while (reader.readLine().also { line = it } != null) {
                            if (line!!.contains("time=")) {
                                val timeStr = line!!.substringAfter("time=").substringBefore(" ms")
                                resultMs = timeStr.toDoubleOrNull() ?: -1.0
                                timeout = false
                            }
                        }
                        process.waitFor()
                    } catch (e: Exception) {
                        logErrorToFile(e, "pingRunnable")
                    } finally {
                        try { process?.destroy() } catch (ignored: Exception) {}
                    }

                    currentPingMs = resultMs
                    isPingTimeout = timeout
                    isPingExecuting = false

                    // Pingの結果が出たら、画面表示を強制アップデートする
                    handler.post { processNetworkState(force = true) }
                }
            }
            handler.postDelayed(this, intervalMs)
        }
    }

    // 画面の点灯・消灯を検知するレシーバー（バッテリー節約のため消灯時は更新を止める）
    private val screenReceiver = object : BroadcastReceiver() {
        override fun onReceive(context: Context?, intent: Intent?) {
            when (intent?.action) {
                Intent.ACTION_SCREEN_OFF -> {
                    isScreenOn = false
                    handler.removeCallbacks(pollingRunnable)
                    handler.removeCallbacks(pingRunnable)
                }
                Intent.ACTION_SCREEN_ON -> {
                    isScreenOn = true
                    ignoreDisconnectUntil = System.currentTimeMillis() + 5000L
                    processNetworkState(force = true)
                    handler.postDelayed(pollingRunnable, getPollingIntervalMs())
                    handler.post(pingRunnable)
                }
            }
        }
    }

    // 通知バーに表示するテキストの一時保存変数
    private var currentTopText = "起動中"
    private var currentBottomText = ""
    private var currentTitle = "通信状態を取得中..."
    private var currentBody = "データ通信未確立"

    @Volatile private var isFetching = false
    private var lastFetchTime = 0L
    private var isEndcAnchor = false // 4Gが5G NSAのアンカーとして働いているかのフラグ

    // 解析してUIに送るための最終的なセル情報データクラス
    private data class ChannelData(val isRegistered: Boolean, var isPrimary: Boolean, val type: String, var band: String, val bwMhz: Int, var earfcn: Int, var pci: Int, var rsrp: Int = UNAVAILABLE_VALUE, var rsrq: Int = UNAVAILABLE_VALUE, var sinr: Int = UNAVAILABLE_VALUE, var ql: Int = 0, var cqi: Int = UNAVAILABLE_VALUE, var ta: Int = UNAVAILABLE_VALUE, var ci: Long = -1L, var mcc: Int = UNAVAILABLE_VALUE, var mnc: Int = UNAVAILABLE_VALUE, var isDuplicate: Boolean = false, var estimatedMbps: Double = 0.0)

    // 標準APIから取得したセル情報データクラス
    private data class ParsedCell(val isRegistered: Boolean, val type: String, val bands: List<String>, val earfcn: Int, val pci: Int, val rsrp: Int, val rsrq: Int, val sinr: Int, val cqi: Int, val ta: Int, val bwKhz: Int, val ci: Long, val mcc: Int, val mnc: Int, var used: Boolean = false)

    // dumpsys（生テキスト）から抽出したセル情報データクラス
    private class DumpsysCell(val isRegistered: Boolean, val type: String, val bands: List<String>, val rsrp: Int, val rsrq: Int, val sinr: Int, val earfcn: Int, val pci: Int, val cqi: Int, val ta: Int, val bwKhz: Int, val ci: Long, val mcc: Int, val mnc: Int, var used: Boolean = false)

    // サービス生成時の初期化処理
    override fun onCreate() {
        super.onCreate()
        createNotificationChannel()
        telephonyManager = getSystemService(Context.TELEPHONY_SERVICE) as TelephonyManager
        registerReceiver(screenReceiver, IntentFilter().apply { addAction(Intent.ACTION_SCREEN_ON); addAction(Intent.ACTION_SCREEN_OFF) })
    }

    // 外部からサービスが呼ばれた（コマンドを受け取った）時の処理
    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        val prefs = getSharedPreferences("FiveGCheckerPrefs", Context.MODE_PRIVATE)
        isCsvLoggingEnabled = prefs.getBoolean("csv_logging_enabled", false)
        isCsvLogAllCells = prefs.getBoolean("csv_log_all_cells", false)

        when (intent?.action) {
            "ACTION_RESET_STATS" -> {
                resetStatsAndHistory()
                processNetworkState(force = true)
            }
            "UPDATE_PREFS", "ACTION_MANUAL_UPDATE" -> {
                if (intent.action == "ACTION_MANUAL_UPDATE") android.widget.Toast.makeText(this, "🔄 データを更新しました", android.widget.Toast.LENGTH_SHORT).show()
                handler.removeCallbacks(pollingRunnable)
                handler.removeCallbacks(pingRunnable)
                processNetworkState(force = true)
                handler.postDelayed(pollingRunnable, getPollingIntervalMs())
                handler.post(pingRunnable)
            }
            else -> {
                // 初回起動時はフォアグラウンドサービス（常駐）として登録
                startForegroundServiceWithNotification()
                startMonitoring()
            }
        }
        return START_STICKY
    }

    private fun startForegroundServiceWithNotification() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.UPSIDE_DOWN_CAKE) {
            startForeground(NOTIFICATION_ID, createNotification(), ServiceInfo.FOREGROUND_SERVICE_TYPE_SPECIAL_USE)
        } else {
            startForeground(NOTIFICATION_ID, createNotification())
        }
    }

    override fun onDestroy() {
        super.onDestroy()
        stopMonitoring()
        unregisterReceiver(screenReceiver)
        removeOverlay()
        stopForeground(STOP_FOREGROUND_REMOVE)
        pingExecutor.shutdown()
    }

    override fun onBind(intent: Intent?): IBinder? { return null }

    // 各種監視（APIコールバック、GPS）の開始
    private fun startMonitoring() {
        try {
            if (ContextCompat.checkSelfPermission(this, Manifest.permission.READ_PHONE_STATE) == PackageManager.PERMISSION_GRANTED) {
                telephonyCallback = NetworkCallback()
                telephonyManager.registerTelephonyCallback(mainExecutor, telephonyCallback!!)
                isScreenOn = true
                handler.post(pollingRunnable)
                handler.post(pingRunnable)

                if (ContextCompat.checkSelfPermission(this, Manifest.permission.ACCESS_FINE_LOCATION) == PackageManager.PERMISSION_GRANTED) {
                    locationManager = getSystemService(Context.LOCATION_SERVICE) as android.location.LocationManager
                    locationManager?.requestLocationUpdates(android.location.LocationManager.GPS_PROVIDER, 2000L, 0f, locationListener)
                }
            }
        } catch (e: Exception) {
            logErrorToFile(e, "startMonitoring")
        }
    }

    private fun stopMonitoring() {
        telephonyCallback?.let { telephonyManager.unregisterTelephonyCallback(it) }
        handler.removeCallbacks(pollingRunnable)
        handler.removeCallbacks(pingRunnable)
        try { locationManager?.removeUpdates(locationListener) } catch (e: Exception) {}
    }

    // 標準APIからネットワークの状態変化（アンカーなど）をリアルタイムで受け取るコールバック
    private inner class NetworkCallback : TelephonyCallback(), TelephonyCallback.DisplayInfoListener {
        override fun onDisplayInfoChanged(telephonyDisplayInfo: TelephonyDisplayInfo) {
            isEndcAnchor = telephonyDisplayInfo.overrideNetworkType == TelephonyDisplayInfo.OVERRIDE_NETWORK_TYPE_NR_NSA || telephonyDisplayInfo.overrideNetworkType == TelephonyDisplayInfo.OVERRIDE_NETWORK_TYPE_NR_ADVANCED
        }
    }

    private fun getPollingIntervalMs(): Long {
        return when (getSharedPreferences("FiveGCheckerPrefs", Context.MODE_PRIVATE).getInt("update_interval", 1)) {
            0 -> 2000L; 1 -> 5000L; 2 -> 10000L; 3 -> 30000L; 4 -> 60000L; else -> 5000L
        }
    }

    private fun isWifiConnected(): Boolean {
        return try {
            val cm = getSystemService(Context.CONNECTIVITY_SERVICE) as ConnectivityManager
            val network = cm.activeNetwork ?: return false
            val caps = cm.getNetworkCapabilities(network) ?: return false
            caps.hasTransport(NetworkCapabilities.TRANSPORT_WIFI)
        } catch (e: Exception) {
            false
        }
    }

    // デュアルSIM対応：現在アクティブなSIMのキャリア番号（MCC/MNC）を取得
    @SuppressLint("MissingPermission")
    private fun getTargetSimPlmn(): Pair<Int, Int>? {
        try {
            if (ContextCompat.checkSelfPermission(this, Manifest.permission.READ_PHONE_STATE) == PackageManager.PERMISSION_GRANTED) {
                val prefs = getSharedPreferences("FiveGCheckerPrefs", Context.MODE_PRIVATE)
                val targetSlot = prefs.getInt("pref_sim_slot", 0)

                val subscriptionManager = getSystemService(Context.TELEPHONY_SUBSCRIPTION_SERVICE) as SubscriptionManager
                val activeSubscriptionInfoList = subscriptionManager.activeSubscriptionInfoList

                if (activeSubscriptionInfoList != null && activeSubscriptionInfoList.isNotEmpty()) {
                    val targetInfo = activeSubscriptionInfoList.find { it.simSlotIndex == targetSlot }
                    if (targetInfo != null) {
                        val mcc = targetInfo.mccString?.toIntOrNull() ?: UNAVAILABLE_VALUE
                        val mnc = targetInfo.mncString?.toIntOrNull() ?: UNAVAILABLE_VALUE
                        if (mcc != UNAVAILABLE_VALUE && mnc != UNAVAILABLE_VALUE) {
                            return Pair(mcc, mnc)
                        }
                    }
                }

                val simOperator = telephonyManager.simOperator
                if (!simOperator.isNullOrEmpty() && simOperator.length >= 5) {
                    val mcc = simOperator.substring(0, 3).toIntOrNull() ?: UNAVAILABLE_VALUE
                    val mnc = simOperator.substring(3).toIntOrNull() ?: UNAVAILABLE_VALUE
                    if (mcc != UNAVAILABLE_VALUE && mnc != UNAVAILABLE_VALUE) {
                        return Pair(mcc, mnc)
                    }
                }
            }
        } catch (e: Exception) {
            logErrorToFile(e, "getTargetSimPlmn")
        }
        return null
    }

    // 標準APIから周辺の電波状況（CellInfo）を安全に取得するメソッド
    @SuppressLint("MissingPermission")
    private fun getStandardCellInfo(): List<ParsedCell> {
        val list = mutableListOf<ParsedCell>()
        try {
            val targetPlmn = getTargetSimPlmn()

            telephonyManager.allCellInfo?.forEach { info ->
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
                    // 4G(LTE)基地局の場合の処理
                    if (info is CellInfoLte) {
                        val id = info.cellIdentity as CellIdentityLte
                        val sig = info.cellSignalStrength as CellSignalStrengthLte
                        val mcc = id.mccString?.toIntOrNull() ?: UNAVAILABLE_VALUE
                        val mnc = id.mncString?.toIntOrNull() ?: UNAVAILABLE_VALUE

                        // 他キャリアの電波を除外するフィルタリング
                        if (targetPlmn != null && mcc != UNAVAILABLE_VALUE && mnc != UNAVAILABLE_VALUE) {
                            if (mcc != targetPlmn.first || mnc != targetPlmn.second) {
                                return@forEach
                            }
                        }

                        val cqi = if (sig.cqi != android.telephony.CellInfo.UNAVAILABLE) sig.cqi else UNAVAILABLE_VALUE
                        val sinr = if (sig.rssnr != android.telephony.CellInfo.UNAVAILABLE) sig.rssnr else UNAVAILABLE_VALUE
                        val ta = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O && sig.timingAdvance != android.telephony.CellInfo.UNAVAILABLE) sig.timingAdvance else UNAVAILABLE_VALUE
                        val bwRaw = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.P) id.bandwidth else android.telephony.CellInfo.UNAVAILABLE
                        val bwKhz = if (bwRaw != android.telephony.CellInfo.UNAVAILABLE) bwRaw else 0
                        val ci = if (id.ci != android.telephony.CellInfo.UNAVAILABLE) id.ci.toLong() else -1L
                        list.add(ParsedCell(info.isRegistered, "4G", id.bands.map { it.toString() }, id.earfcn, id.pci, sig.rsrp, sig.rsrq, sinr, cqi, ta, bwKhz, ci, mcc, mnc))

                        // 5G(NR)基地局の場合の処理
                    } else if (info is CellInfoNr) {
                        val id = info.cellIdentity as CellIdentityNr
                        val sig = info.cellSignalStrength as CellSignalStrengthNr
                        val rsrp = if (sig.csiRsrp != android.telephony.CellInfo.UNAVAILABLE) sig.csiRsrp else sig.ssRsrp
                        val rsrq = if (sig.csiRsrq != android.telephony.CellInfo.UNAVAILABLE) sig.csiRsrq else sig.ssRsrq
                        val mcc = id.mccString?.toIntOrNull() ?: UNAVAILABLE_VALUE
                        val mnc = id.mncString?.toIntOrNull() ?: UNAVAILABLE_VALUE

                        if (targetPlmn != null && mcc != UNAVAILABLE_VALUE && mnc != UNAVAILABLE_VALUE) {
                            if (mcc != targetPlmn.first || mnc != targetPlmn.second) {
                                return@forEach
                            }
                        }

                        val cqi = sig.csiCqiReport.firstOrNull() ?: UNAVAILABLE_VALUE
                        val sinr = if (sig.csiSinr != android.telephony.CellInfo.UNAVAILABLE) sig.csiSinr else if (sig.ssSinr != android.telephony.CellInfo.UNAVAILABLE) sig.ssSinr else UNAVAILABLE_VALUE
                        val nci = if (id.nci != Long.MAX_VALUE) id.nci else -1L
                        list.add(ParsedCell(info.isRegistered, "5G", id.bands.map { it.toString() }, id.nrarfcn, id.pci, rsrp, rsrq, sinr, cqi, UNAVAILABLE_VALUE, 0, nci, mcc, mnc))
                    }
                }
            }
        } catch (e: Exception) {
            logErrorToFile(e, "getStandardCellInfo")
        }
        return list
    }

    private fun addHistoryEvent(msg: String, colorHex: String) {
        val timeStr = SimpleDateFormat("HH:mm:ss", Locale.getDefault()).format(Date())
        historyList.add(0, HistoryEvent(timeStr, msg, colorHex))
        if (historyList.size > 200) historyList.removeAt(historyList.lastIndex)
    }

    // 接続している5Gが「4G周波数からの転用」かどうかを判定する処理
    private fun isTenyo5G(band: String, arfcn: Int): Boolean {
        val cleanBand = band.replace(Regex("[^0-9]"), "")
        return when (cleanBand) {
            "79", "257", "40" -> false
            "77", "78" -> if (arfcn > 0) arfcn < 640000 else false
            else -> true
        }
    }

    // 【データの取得開始トリガー】
    // このメソッドからShizuku経由のdumpsys取得とデータ解析
    private fun processNetworkState(force: Boolean = false) {
        if (Shizuku.checkSelfPermission() != PackageManager.PERMISSION_GRANTED) return
        if (isFetching) return
        val currentTime = System.currentTimeMillis()
        if (!force && (lastFetchTime != 0L && currentTime - lastFetchTime < 1000)) return
        isFetching = true; lastFetchTime = currentTime

        Thread {
            var process: java.lang.Process? = null
            try {
                // 1. まず標準APIから基礎データを集める
                val standardCells = getStandardCellInfo()
                // 2. Shizuku（システム権限）を使って「dumpsys telephony.registry」コマンドを強制実行する
                val newProcessMethod = Shizuku::class.java.getDeclaredMethod("newProcess", Array<String>::class.java, Array<String>::class.java, String::class.java)
                newProcessMethod.isAccessible = true
                process = newProcessMethod.invoke(null, arrayOf("dumpsys", "telephony.registry"), null, null) as java.lang.Process
                val reader = BufferedReader(InputStreamReader(process.inputStream))

                // 3. 取得した生テキストを解析へ回す
                parseAndFormatData(reader.readText(), standardCells, currentTime)
            } catch (e: Exception) {
                logErrorToFile(e, "processNetworkState")
            } finally {
                try { process?.destroy() } catch (e: Exception) {}
                isFetching = false
            }
        }.start()
    }

    // dumpsysの生テキストを正規表現で解析し、必要な情報にまとめる処理
    private fun parseAndFormatData(dumpsysRaw: String, standardCells: List<ParsedCell>, fetchTimeMs: Long) {
        val prefs = getSharedPreferences("FiveGCheckerPrefs", Context.MODE_PRIVATE)
        val channels = mutableListOf<ChannelData>()
        val dumpsysPool = mutableListOf<DumpsysCell>()

        // 1. デュアルSIM対応: 選択されたSIM（Phone Id）のブロックだけを切り出す
        val targetPhoneId = prefs.getInt("pref_sim_slot", 0)
        var targetPhoneBlock = dumpsysRaw
        try {
            if (targetPhoneId == 0) {
                targetPhoneBlock = Regex("Phone Id=0(.*?)Phone Id=1", RegexOption.DOT_MATCHES_ALL).find(dumpsysRaw)?.groupValues?.get(1)
                    ?: Regex("Phone Id=0(.*)", RegexOption.DOT_MATCHES_ALL).find(dumpsysRaw)?.groupValues?.get(1) ?: dumpsysRaw
            } else {
                targetPhoneBlock = Regex("Phone Id=1(.*?)Phone Id=2", RegexOption.DOT_MATCHES_ALL).find(dumpsysRaw)?.groupValues?.get(1)
                    ?: Regex("Phone Id=1(.*)", RegexOption.DOT_MATCHES_ALL).find(dumpsysRaw)?.groupValues?.get(1) ?: dumpsysRaw
            }
        } catch (e: Exception) {
            logErrorToFile(e, "parseAndFormatData_PhoneIdBlock")
        }

        var isEmergency = false
        if (targetPhoneBlock.isNotEmpty()) {
            isEmergency = targetPhoneBlock.contains("mIsEmergencyOnly=true") || targetPhoneBlock.contains("registrationState=EMERGENCY")
        }

        // 2. 基地局ID（PCI）と周波数帯（Band, EARFCN）のマッピングを先に作っておく
        val pciToBandMap = mutableMapOf<Int, Pair<String, Int>>()
        val idMatch = Regex("CellIdentity[A-Za-z]+:\\{([^}]+)\\}").findAll(targetPhoneBlock)
        for (match in idMatch) {
            val idBlock = match.groupValues[1]
            val p = Regex("mPci\\s*=?\\s*\\[?(\\d+)\\]?").find(idBlock)?.groupValues?.get(1)?.toIntOrNull() ?: continue
            val a = Regex("(?:mEarfcn|mNrarfcn|mNrArfcn|mUarfcn)\\s*=?\\s*\\[?(\\d+)\\]?").find(idBlock)?.groupValues?.get(1)?.toIntOrNull() ?: -1
            val bStr = Regex("mBands\\s*=\\s*\\[(.*?)\\]").find(idBlock)?.groupValues?.get(1) ?: ""
            val fB = bStr.split(",").firstOrNull()?.trim() ?: ""
            if (fB.isNotEmpty() && fB != "0") pciToBandMap[p] = Pair(fB, a)
        }

        // 3. テキスト内の「CellInfo」ブロック（RSRPやCQIなど）を1つずつ抽出してプール（dumpsysPool）に貯める
        val cellBlocks = targetPhoneBlock.split(Regex("CellInfo[Lte|Nr]+:\\{")).drop(1)
        for (block in cellBlocks) {
            val isReg = block.contains("mRegistered=YES")
            val type = if (block.contains("ssRsrp") || block.contains("csiRsrp")) "5G" else "4G"
            val rsrp = Regex("(?:csiRsrp|ssRsrp|rsrp)\\s*=\\s*([-\\d]+)").findAll(block).mapNotNull { it.groupValues[1].toIntOrNull() }.firstOrNull { it < 0 } ?: UNAVAILABLE_VALUE
            val rsrq = Regex("(?:csiRsrq|ssRsrq|rsrq)\\s*=\\s*([-\\d]+)").findAll(block).mapNotNull { it.groupValues[1].toIntOrNull() }.firstOrNull { it < 0 } ?: UNAVAILABLE_VALUE

            val sinrLte = Regex("rssnr\\s*=\\s*([-\\d]+)").find(block)?.groupValues?.get(1)?.toIntOrNull()
            val sinrNr = Regex("(?:csiSinr|ssSinr)\\s*=\\s*([-\\d]+)").findAll(block).mapNotNull { it.groupValues[1].toIntOrNull() }.firstOrNull()
            val sinr = sinrLte ?: sinrNr ?: UNAVAILABLE_VALUE

            val bandsStr = Regex("mBands=\\[(.*?)\\]").find(block)?.groupValues?.get(1) ?: ""

            val earfcn = Regex("(?:mEarfcn|mNrarfcn|mNrArfcn)\\s*=?\\s*\\[?(\\d+)\\]?").find(block)?.groupValues?.get(1)?.toIntOrNull() ?: -1
            val pci = Regex("mPci\\s*=?\\s*\\[?(\\d+)\\]?").find(block)?.groupValues?.get(1)?.toIntOrNull() ?: -1

            val bwKhz = Regex("mBandwidth\\s*=?\\s*\\[?(\\d+)\\]?").find(block)?.groupValues?.get(1)?.toIntOrNull() ?: 0

            val cqiLte = Regex("cqi=([\\d]+)").find(block)?.groupValues?.get(1)?.toIntOrNull() ?: UNAVAILABLE_VALUE
            val cqiNr = Regex("csiCqiReport = \\[([\\d]+)\\]").find(block)?.groupValues?.get(1)?.toIntOrNull() ?: UNAVAILABLE_VALUE
            val cqi = if(cqiLte != UNAVAILABLE_VALUE) cqiLte else cqiNr

            val taLte = Regex("ta=([\\d]+)").find(block)?.groupValues?.get(1)?.toIntOrNull() ?: UNAVAILABLE_VALUE
            val taNr = Regex("timingAdvance\\s*=\\s*([\\d]+)").find(block)?.groupValues?.get(1)?.toIntOrNull() ?: UNAVAILABLE_VALUE
            val ta = if (taLte != UNAVAILABLE_VALUE) taLte else taNr

            val ci = Regex("(?:mCi|mNci)\\s*=?\\s*\\[?(\\d+)\\]?").find(block)?.groupValues?.get(1)?.toLongOrNull() ?: -1L

            val mcc = Regex("mMcc=(null|\\d+)").find(block)?.groupValues?.get(1)?.toIntOrNull() ?: UNAVAILABLE_VALUE
            val mnc = Regex("mMnc=(null|\\d+)").find(block)?.groupValues?.get(1)?.toIntOrNull() ?: UNAVAILABLE_VALUE

            dumpsysPool.add(DumpsysCell(isReg, type, bandsStr.split(",").map { it.trim() }.filter { it.isNotEmpty() }, rsrp, rsrq, sinr, earfcn, pci, cqi, ta, bwKhz, ci, mcc, mnc))
        }

        // 4. キャリアアグリゲーション（CA）の構成を特定する
        val pccBlocks = targetPhoneBlock.split("mConnectionStatus=").drop(1)
        for (block in pccBlocks) {
            val isPrimary = block.startsWith("PrimaryServing") // メインで通信しているセル（PCell）か
            val isSecondary = block.startsWith("SecondaryServing") // CAで束ねているセル（SCell）か
            if (!isPrimary && !isSecondary) continue

            // 帯域幅(BW)や周波数などの骨格となるデータを抽出
            val bwKhz = Regex("mCellBandwidthDownlinkKhz=(\\d+)").find(block)?.groupValues?.get(1)?.toIntOrNull() ?: 0
            val type = if ((Regex("mNetworkType=([A-Za-z]+)").find(block)?.groupValues?.get(1) ?: "LTE") == "NR") "5G" else "4G"
            val earfcn = Regex("mDownlinkChannelNumber=(\\d+)").find(block)?.groupValues?.get(1)?.toIntOrNull() ?: -1
            val pci = Regex("mPhysicalCellId=(-?\\d+)").find(block)?.groupValues?.get(1)?.toIntOrNull() ?: -1
            val band = Regex("mBand=(\\d+)").find(block)?.groupValues?.get(1) ?: "?"
            val ch = ChannelData(true, isPrimary, type, band, bwKhz / 1000, earfcn, pci)

            // バンド名が不明な場合は、先ほど作ったマップから推測して補完
            if (ch.band == "0" || ch.band == "?" || ch.band == "2147483647") {
                pciToBandMap[ch.pci]?.let { fallback ->
                    ch.band = fallback.first
                    if (ch.earfcn <= 0 || ch.earfcn == UNAVAILABLE_VALUE) ch.earfcn = fallback.second
                }
            }

            var matchRsrp = UNAVAILABLE_VALUE
            var matchRsrq = UNAVAILABLE_VALUE
            var matchSinr = UNAVAILABLE_VALUE
            var matchCqi = UNAVAILABLE_VALUE
            var matchTa = UNAVAILABLE_VALUE
            var matchCi = -1L
            var matchMcc = UNAVAILABLE_VALUE
            var matchMnc = UNAVAILABLE_VALUE

            // 5. CAの骨格に対して、標準APIのデータから「RSRPなどの肉付け」を探して合体させる
            var stdMatch = standardCells.find { !it.used && it.pci == ch.pci && ch.pci > 0 }
                ?: standardCells.find { !it.used && it.earfcn == ch.earfcn && ch.earfcn > 0 }

            if (stdMatch != null && stdMatch.rsrp != UNAVAILABLE_VALUE) {
                stdMatch.used = true // 使用済みフラグを立てて、他のセルへのコピー（誤表示）を防ぐ
                matchRsrp = stdMatch.rsrp; matchRsrq = stdMatch.rsrq; matchSinr = stdMatch.sinr; matchCqi = stdMatch.cqi; matchTa = stdMatch.ta; matchCi = stdMatch.ci; matchMcc = stdMatch.mcc; matchMnc = stdMatch.mnc
            } else {
                // 標準APIで見つからなければ、先ほどプールしたdumpsys側のデータから探す
                var dumpMatch = dumpsysPool.find { !it.used && it.pci == ch.pci && ch.pci > 0 }
                    ?: dumpsysPool.find { !it.used && it.earfcn == ch.earfcn && ch.earfcn > 0 }

                if (dumpMatch != null) {
                    dumpMatch.used = true;
                    matchRsrp = dumpMatch.rsrp; matchRsrq = dumpMatch.rsrq; matchSinr = dumpMatch.sinr; matchCqi = dumpMatch.cqi; matchTa = dumpMatch.ta; matchCi = dumpMatch.ci; matchMcc = dumpMatch.mcc; matchMnc = dumpMatch.mnc
                }
            }

            ch.rsrp = matchRsrp; ch.rsrq = matchRsrq; ch.sinr = matchSinr; ch.cqi = matchCqi; ch.ta = matchTa; ch.ci = matchCi; ch.mcc = matchMcc; ch.mnc = matchMnc
            calculateQualityAndSpeed(ch) // 品質スコアなどを計算
            channels.add(ch) // 完成したセルデータをリストに追加
        }

        // 6. CA（骨格）に乗らなかった余りのデータがあれば、周辺セルとしてリストに追加
        val unusedRegisteredDumps = dumpsysPool.filter {
            !it.used && it.isRegistered &&
                    it.earfcn > 0 && it.earfcn != UNAVAILABLE_VALUE &&
                    it.pci > 0 && it.pci != UNAVAILABLE_VALUE
        }
        for (dump in unusedRegisteredDumps) {
            val hasPrimary = channels.any { it.isPrimary }
            val isPri = !hasPrimary

            val ch = ChannelData(true, isPri, dump.type, dump.bands.firstOrNull() ?: "?", dump.bwKhz / 1000, dump.earfcn, dump.pci)
            ch.rsrp = dump.rsrp
            ch.rsrq = dump.rsrq
            ch.sinr = dump.sinr
            ch.cqi = dump.cqi
            ch.ta = dump.ta
            ch.ci = dump.ci
            ch.mcc = dump.mcc
            ch.mnc = dump.mnc
            calculateQualityAndSpeed(ch)
            channels.add(ch)
            dump.used = true
        }

        // 同様に標準APIの余りも処理
        val unusedStdCells = standardCells.filter {
            !it.used && it.isRegistered &&
                    it.earfcn > 0 && it.earfcn != UNAVAILABLE_VALUE &&
                    it.pci > 0 && it.pci != UNAVAILABLE_VALUE
        }

        for (std in unusedStdCells) {
            val hasPrimary = channels.any { it.isPrimary }
            val isPri = !hasPrimary

            val ch = ChannelData(true, isPri, std.type, std.bands.firstOrNull() ?: "?", std.bwKhz / 1000, std.earfcn, std.pci)
            ch.rsrp = std.rsrp
            ch.rsrq = std.rsrq
            ch.sinr = std.sinr
            ch.cqi = std.cqi
            ch.ta = std.ta
            ch.ci = std.ci
            ch.mcc = std.mcc
            ch.mnc = std.mnc
            calculateQualityAndSpeed(ch)
            channels.add(ch)
        }

        // もしデータが完全に空だった場合の最終手段フォールバック
        if (channels.isEmpty()) {
            val primaryStd = standardCells.find { it.isRegistered }
            if (primaryStd != null && primaryStd.bands.isNotEmpty()) {
                val band = primaryStd.bands.first()
                val bwKhz = Regex("mRegistered=YES.*?mBandwidth=(\\d+)").find(targetPhoneBlock)?.groupValues?.get(1)?.toIntOrNull() ?: 0
                val type = if (band == "78" || band == "79" || band.toIntOrNull()?.let { it > 100 } == true) "5G" else "4G"
                val ch = ChannelData(true, true, type, band, bwKhz / 1000, primaryStd.earfcn, primaryStd.pci, primaryStd.rsrp, primaryStd.rsrq, primaryStd.sinr, 0, primaryStd.cqi, primaryStd.ta, primaryStd.ci, primaryStd.mcc, primaryStd.mnc)
                calculateQualityAndSpeed(ch)
                channels.add(ch)
            }
        }

        // 7. PCell（主回線）や5Gセルの細かいRSRP/RSRQなどが欠落している場合、テキストから再取得を試みる
        channels.firstOrNull { it.isPrimary }?.let { pcell ->
            if (pcell.rsrp == UNAVAILABLE_VALUE || pcell.rsrq == UNAVAILABLE_VALUE || pcell.sinr == UNAVAILABLE_VALUE ||
                pcell.cqi == UNAVAILABLE_VALUE || pcell.ta == UNAVAILABLE_VALUE || pcell.ta == -1) {
                try {
                    var rsrpStr: String? = null
                    var rsrqStr: String? = null
                    var sinrStr: String? = null

                    if (pcell.type == "4G") {
                        rsrpStr = Regex("mLte=CellSignalStrengthLte:.*?rsrp=([-\\d]+)").find(targetPhoneBlock)?.groupValues?.get(1)
                        rsrqStr = Regex("mLte=CellSignalStrengthLte:.*?rsrq=([-\\d]+)").find(targetPhoneBlock)?.groupValues?.get(1)
                        sinrStr = Regex("mLte=CellSignalStrengthLte:.*?rssnr=([-\\d]+)").find(targetPhoneBlock)?.groupValues?.get(1)
                        val cqiLte = Regex("mLte=CellSignalStrengthLte:.*?cqi=([\\d]+)").find(targetPhoneBlock)?.groupValues?.get(1)?.toIntOrNull()
                        val taLte = Regex("mLte=CellSignalStrengthLte:.*?ta=([\\d]+)").find(targetPhoneBlock)?.groupValues?.get(1)?.toIntOrNull()

                        if (pcell.cqi == UNAVAILABLE_VALUE && cqiLte != null && cqiLte != UNAVAILABLE_VALUE) pcell.cqi = cqiLte
                        if ((pcell.ta == UNAVAILABLE_VALUE || pcell.ta == -1) && taLte != null && taLte != UNAVAILABLE_VALUE) pcell.ta = taLte
                    } else {
                        rsrpStr = Regex("mNr=CellSignalStrengthNr:.*?ssRsrp = ([-\\d]+)").find(targetPhoneBlock)?.groupValues?.get(1)
                        rsrqStr = Regex("mNr=CellSignalStrengthNr:.*?ssRsrq = ([-\\d]+)").find(targetPhoneBlock)?.groupValues?.get(1)
                        sinrStr = Regex("mNr=CellSignalStrengthNr:.*?ssSinr = ([-\\d]+)").find(targetPhoneBlock)?.groupValues?.get(1)
                        val cqiNr = Regex("mNr=CellSignalStrengthNr:.*?csiCqiReport = \\[(.*?)\\]").find(targetPhoneBlock)?.groupValues?.get(1)?.toIntOrNull()
                        val taNr = Regex("mNr=CellSignalStrengthNr:.*?timingAdvance\\s*=\\s*([\\d]+)").find(targetPhoneBlock)?.groupValues?.get(1)?.toIntOrNull()

                        if (pcell.cqi == UNAVAILABLE_VALUE && cqiNr != null && cqiNr != UNAVAILABLE_VALUE) pcell.cqi = cqiNr
                        if ((pcell.ta == UNAVAILABLE_VALUE || pcell.ta == -1) && taNr != null && taNr != UNAVAILABLE_VALUE) pcell.ta = taNr
                    }

                    if (pcell.rsrp == UNAVAILABLE_VALUE) rsrpStr?.toIntOrNull()?.let { if (it < 0 && it != UNAVAILABLE_VALUE) pcell.rsrp = it }
                    if (pcell.rsrq == UNAVAILABLE_VALUE) rsrqStr?.toIntOrNull()?.let { if (it < 0 && it != UNAVAILABLE_VALUE) pcell.rsrq = it }
                    if (pcell.sinr == UNAVAILABLE_VALUE) sinrStr?.toIntOrNull()?.let { if (it != UNAVAILABLE_VALUE) pcell.sinr = it }

                    calculateQualityAndSpeed(pcell)
                } catch (e: Exception) {
                    logErrorToFile(e, "parseAndFormatData_pcellFallback")
                }
            }
        }

        // 5GのSCellへのフォールバック
        channels.filter { it.type == "5G" && !it.isPrimary &&
                (it.rsrp == UNAVAILABLE_VALUE || it.rsrq == UNAVAILABLE_VALUE || it.sinr == UNAVAILABLE_VALUE ||
                        it.cqi == UNAVAILABLE_VALUE || it.ta == UNAVAILABLE_VALUE || it.ta == -1) }.forEach { nrCell ->
            try {
                val rsrpStr = Regex("mNr=CellSignalStrengthNr:.*?ssRsrp = ([-\\d]+)").find(targetPhoneBlock)?.groupValues?.get(1)
                val rsrqStr = Regex("mNr=CellSignalStrengthNr:.*?ssRsrq = ([-\\d]+)").find(targetPhoneBlock)?.groupValues?.get(1)
                val sinrStr = Regex("mNr=CellSignalStrengthNr:.*?ssSinr = ([-\\d]+)").find(targetPhoneBlock)?.groupValues?.get(1)
                val cqiNr = Regex("mNr=CellSignalStrengthNr:.*?csiCqiReport = \\[(.*?)\\]").find(targetPhoneBlock)?.groupValues?.get(1)?.toIntOrNull()
                val taNr = Regex("mNr=CellSignalStrengthNr:.*?timingAdvance\\s*=\\s*([\\d]+)").find(targetPhoneBlock)?.groupValues?.get(1)?.toIntOrNull()

                if (nrCell.cqi == UNAVAILABLE_VALUE && cqiNr != null && cqiNr != UNAVAILABLE_VALUE) nrCell.cqi = cqiNr
                if ((nrCell.ta == UNAVAILABLE_VALUE || nrCell.ta == -1) && taNr != null && taNr != UNAVAILABLE_VALUE) nrCell.ta = taNr

                if (nrCell.rsrp == UNAVAILABLE_VALUE) rsrpStr?.toIntOrNull()?.let { if (it < 0 && it != UNAVAILABLE_VALUE) nrCell.rsrp = it }
                if (nrCell.rsrq == UNAVAILABLE_VALUE) rsrqStr?.toIntOrNull()?.let { if (it < 0 && it != UNAVAILABLE_VALUE) nrCell.rsrq = it }
                if (nrCell.sinr == UNAVAILABLE_VALUE) sinrStr?.toIntOrNull()?.let { if (it != UNAVAILABLE_VALUE) nrCell.sinr = it }

                calculateQualityAndSpeed(nrCell)
            } catch (e: Exception) {
                logErrorToFile(e, "parseAndFormatData_nrCellFallback")
            }
        }

        // 緊急通報のみモードだった場合の処理
        if (isEmergency) {
            try {
                val plmnMatch = Regex("registrationState=EMERGENCY.*?mMcc\\s*=\\s*(\\d+).*?mMnc\\s*=\\s*(\\d+)", RegexOption.DOT_MATCHES_ALL).find(targetPhoneBlock)
                    ?: Regex("mMcc\\s*=\\s*(\\d+).*?mMnc\\s*=\\s*(\\d+)").find(targetPhoneBlock)

                if (plmnMatch != null) {
                    val eMcc = plmnMatch.groupValues[1].toInt()
                    val eMnc = plmnMatch.groupValues[2].toInt()
                    channels.forEach { ch ->
                        ch.mcc = eMcc
                        ch.mnc = eMnc
                    }
                }
            } catch (e: Exception) {
                logErrorToFile(e, "parseAndFormatData_EmergencyPLMN")
            }
        }

        // 周辺セル（NCell）の表示設定がオンならリストに追加する
        if (prefs.getInt("ncell_display", 0) == 1) {
            standardCells.filter { !it.isRegistered }.forEach { std ->
                var bandStr = std.bands.firstOrNull() ?: ""

                val mccFinal = if (isEmergency && channels.isNotEmpty()) channels.first().mcc else std.mcc
                val mncFinal = if (isEmergency && channels.isNotEmpty()) channels.first().mnc else std.mnc

                // バンド名が不明な場合は周波数(EARFCN)から推測する
                if (bandStr.isEmpty() || bandStr == "?") {
                    val guessed = guessBandFromArfcn(std.earfcn, std.type == "5G", mccFinal, mncFinal)
                    bandStr = if (guessed != "?") "$guessed?" else "?"
                }

                val ch = ChannelData(false, false, std.type, bandStr, 0, std.earfcn, std.pci, std.rsrp, std.rsrq, std.sinr, 0, std.cqi, std.ta, std.ci, mccFinal, mncFinal)
                calculateQualityAndSpeed(ch)
                channels.add(ch)
            }
        }

        // 5G NSAの「アンカー状態」かどうかを判定（overrideNetworkType を確認）
        val teleDisplayInfo = Regex("mTelephonyDisplayInfo=TelephonyDisplayInfo \\{network=([A-Za-z0-9_]+), overrideNetwork=([A-Za-z0-9_]+)").find(targetPhoneBlock)
        val networkStr = teleDisplayInfo?.groupValues?.get(1) ?: ""
        val overrideStr = teleDisplayInfo?.groupValues?.get(2) ?: ""

        if (overrideStr == "NR_NSA" || overrideStr == "NR_ADVANCED") {
            isEndcAnchor = true
        }
        if (networkStr == "LTE") {
            // もしシステムがLTE接続だと言っているのに5Gセルがあるなら、それはPCellではない
            channels.filter { it.isRegistered && it.type == "5G" }.forEach { it.isPrimary = false }
        }

        // 不完全な（ゴミデータ）セルを除外する
        val validChannels = mutableListOf<ChannelData>()
        for (ch in channels) {
            val isGhost = (ch.earfcn <= 0 || ch.pci <= 0 || ch.earfcn == UNAVAILABLE_VALUE || ch.pci == UNAVAILABLE_VALUE) && !isEmergency
            if (!isGhost) {
                validChannels.add(ch)
            }
        }

        // 同一周波数・同PCIの重複セルを整理する
        val mergedMap = mutableMapOf<String, ChannelData>()
        for (ch in validChannels) {
            val key = "${ch.type}_${ch.earfcn}_${ch.pci}_${ch.isRegistered}"
            val existing = mergedMap[key]
            if (existing == null) {
                mergedMap[key] = ch
            } else {
                if (!existing.isPrimary && ch.isPrimary) {
                    mergedMap[key] = ch
                } else if (existing.isPrimary == ch.isPrimary) {
                    if (existing.rsrp == UNAVAILABLE_VALUE && ch.rsrp != UNAVAILABLE_VALUE) {
                        mergedMap[key] = ch
                    }
                }
            }
        }
        channels.clear()
        channels.addAll(mergedMap.values)

        // 圏外（Out of Service）判定と、一瞬のデータ欠落を防ぐためのスタンバイ保持処理
        var activeChannels = channels.filter { it.isRegistered }
        val inService = targetPhoneBlock.contains("mDataRegState=0") || targetPhoneBlock.contains("mVoiceRegState=0")

        val strictStandby = prefs.getBoolean("strict_standby", false)
        if (activeChannels.isEmpty() && inService && !isEmergency && !strictStandby) {
            if (lastActiveChannels.isNotEmpty()) {
                activeChannels = lastActiveChannels.map { it.copy() }
                channels.addAll(0, activeChannels)
            }
        } else if (activeChannels.isNotEmpty() && !isEmergency) {
            lastActiveChannels = activeChannels.map { it.copy() }
        }

        val isOutOfService = (!inService && activeChannels.isEmpty()) || isEmergency

        // パケ詰まりアラートやPing結果の文字列生成
        var pingDisplayStr = ""
        var pingAlertStr = ""
        val isPingActive = prefs.getInt("ping_freq", 0) > 0
        if (isPingActive && prefs.getBoolean("ping_display", true) && !isEmergency) {
            if (isPingTimeout) {
                pingDisplayStr = "[Ping: T.O] "
                pingAlertStr = " ⚠️パケ詰(無応答)"
            } else if (currentPingMs >= 0) {
                pingDisplayStr = "[Ping: ${currentPingMs.toInt()}ms] "
                if (currentPingMs > 1000) pingAlertStr = " ⚠️パケ詰(遅延)"
            }
        }

        val isCurrentlyDisconnected = (isOutOfService || (isPingActive && isPingTimeout)) && !isEmergency

        // タイムライン履歴へのイベント追加（通信断、復旧）
        if (isCurrentlyDisconnected && !wasDisconnected) {
            if (fetchTimeMs > ignoreDisconnectUntil) {
                val reason = if (isPingTimeout) "Ping応答なし" else "圏外"
                addHistoryEvent("❌ 通信断 ($reason)", "#E57373")
                wasDisconnected = true
            }
        } else if (!isCurrentlyDisconnected && wasDisconnected) {
            addHistoryEvent("✅ 通信復旧", "#81C784")
            wasDisconnected = false
        }

        // 圏外や待機時の表示処理
        if (activeChannels.isEmpty()) {
            if (isWifiConnected() && prefs.getBoolean("ping_on_wifi", false)) {
                forceUpdateNotificationData("Wi-Fi", "", "Wi-Fi接続中", "Ping監視中...")
                updateOverlay("Wi-Fi接続中$pingAlertStr", pingDisplayStr, emptyList())
                sendUiUpdateIntent(emptyList(), "Wi-Fi接続中", dumpsysRaw, standardCells, dumpsysPool)
                return
            } else if (inService && !isEmergency) {
                forceUpdateNotificationData("待機", "", "電波情報取得中...", "データ取得待ち")
                removeOverlay()
                sendUiUpdateIntent(emptyList(), "電波情報取得中...", dumpsysRaw, standardCells, dumpsysPool)
                return
            } else if (!isEmergency) {
                forceUpdateNotificationData("圏外", "", "圏外", "データ通信未確立")
                removeOverlay()
                sendUiUpdateIntent(emptyList(), "圏外", dumpsysRaw, standardCells, dumpsysPool)
                return
            }
        }

        // --------------------- ここからUIに表示する最終整形処理 ---------------------
        val displayMode = prefs.getInt("display_mode", 1)
        val txPowerIdx = prefs.getInt("tx_power", 1)
        val envIdx = prefs.getInt("env_mode", 1)

        // モデムから取得した推定システム最大速度(Mbps)
        var dlMbps = 0.0
        var ulMbps = 0.0
        val dlMatches = Regex("mDownlinkCapacityKbps=(\\d+)").findAll(targetPhoneBlock).mapNotNull { it.groupValues[1].toIntOrNull() }.filter { it > 0 }
        val ulMatches = Regex("mUplinkCapacityKbps=(\\d+)").findAll(targetPhoneBlock).mapNotNull { it.groupValues[1].toIntOrNull() }.filter { it > 0 }
        if (dlMatches.any()) dlMbps = dlMatches.maxOrNull()!! / 1000.0
        if (ulMatches.any()) ulMatches.maxOrNull()?.let { ulMbps = it / 1000.0 }

        // 接続ネットワークの種類（アイコン名とタイトル）を決定
        var iconText = ""
        var titleBase = ""
        val hasNR = activeChannels.any { it.type == "5G" }
        val isSa = activeChannels.any { it.isPrimary && it.type == "5G" }
        val isCa = activeChannels.size > 1
        val isTenyo = activeChannels.any { it.type == "5G" && isTenyo5G(it.band, it.earfcn) }

        var passiveCongestionAlert = "" // (現在は無効化されている推測パケ詰まりアラート用)

        val finalAlertStr = if (pingAlertStr.isNotEmpty()) pingAlertStr else passiveCongestionAlert

        if (finalAlertStr.isNotEmpty() && lastAlertState != finalAlertStr) {
            addHistoryEvent("${finalAlertStr.trim()} 発生", "#FFF176")
            lastAlertState = finalAlertStr
        } else if (finalAlertStr.isEmpty() && lastAlertState.isNotEmpty()) {
            addHistoryEvent("✅ 通信回復 (${lastAlertState.trim()} 解消)", "#81C784")
            lastAlertState = ""
        }

        // タイムライン履歴へのイベント追加（バンド移動、5G SA接続）
        val pcell = activeChannels.firstOrNull { it.isPrimary }
        if (pcell != null && !isEmergency) {
            val currentPCellKey = "${pcell.type}_${pcell.band}_${pcell.pci}"
            if (lastPCellKey.isNotEmpty() && lastPCellKey != currentPCellKey) {
                if (pcell.type == "5G" && !wasSaConnected) {
                    addHistoryEvent("🚀 5G SA接続確立: n${pcell.band} (PCI:${pcell.pci})", "#64B5F6")
                } else {
                    val prefix = if(pcell.type == "5G") "n" else "B"
                    addHistoryEvent("🔄 移動: $prefix${pcell.band} (PCI:${pcell.pci})", "#EEEEEE")
                }
            }
            lastPCellKey = currentPCellKey
            wasSaConnected = (pcell.type == "5G")
        }

        // 統計データの更新（各バンドの接続時間を加算）
        if (lastStatsUpdateTime > 0 && !isEmergency) {
            val deltaMs = fetchTimeMs - lastStatsUpdateTime
            if (deltaMs in 1..60000 && !isCurrentlyDisconnected) {
                activeChannels.forEach { ch ->
                    val freqStr = getFrequencyFromArfcn(ch.band, ch.earfcn, ch.type == "5G")
                    val isTenyoStat = ch.type == "5G" && isTenyo5G(ch.band, ch.earfcn)
                    val statType = if (isTenyoStat) "転用5G" else ch.type

                    val key = "${statType}_${ch.band}_$freqStr"
                    val stat = bandStats.getOrPut(key) { BandStat(statType, ch.band, freqStr, 0L, 0L) }
                    if (ch.isPrimary) stat.pTimeMs += deltaMs else stat.sTimeMs += deltaMs
                }
            }
        }
        lastStatsUpdateTime = fetchTimeMs

        // 全体の帯域幅と速度の集計
        val totalBandwidth = activeChannels.sumOf { it.bwMhz }
        val totalEffectiveBw = activeChannels.sumOf { (it.bwMhz * (it.ql / 10.0)).toInt() }

        var theoreticalMbps = 0.0
        activeChannels.forEach { ch ->
            theoreticalMbps += ch.estimatedMbps
        }

        val titleSuffixShort = "計${totalBandwidth}M(${totalEffectiveBw}M)"
        val titleSuffixLong = "計${totalBandwidth}MHz(品質${totalEffectiveBw}MHz・推定上限${String.format("%.1f", theoreticalMbps)}Mbps)"
        val speedStrShort = if (dlMbps > 0 || ulMbps > 0) " | ⬇${String.format("%.1f", dlMbps)}M ⬆${String.format("%.1f", ulMbps)}M" else ""
        val speedStrLong = if (dlMbps > 0 || ulMbps > 0) "\n　Link Capacity　↓ ${String.format("%.1f", dlMbps)}Mbps ↑${String.format("%.1f", ulMbps)}Mbps" else ""

        // ネットワークの種類ごとにアイコン名とタイトルを設定
        if (isEmergency) { iconText = "緊急"; titleBase = "緊急通報専用モード" }
        else if (isSa && isCa) { iconText = if (displayMode == 1) "5G/SA" else "5G"; titleBase = "5G SA CA" }
        else if (isSa && !isCa) { iconText = if (displayMode == 1) (if (isTenyo) "5転/SA" else "5G/SA") else "5G"; titleBase = if (isTenyo) "転用5G SA" else "5G SA" }
        else if (hasNR && isTenyo) { iconText = if (displayMode == 1) "5転/NSA" else "5G"; titleBase = "転用5G NSA" }
        else if (hasNR && !isTenyo) { iconText = if (displayMode == 1) "5G/NSA" else "5G"; titleBase = "5G NSA" }
        else if (!hasNR && isCa && isEndcAnchor) { iconText = if (displayMode == 1) "4G+/eLTE" else "4G+"; titleBase = "4G CA(アンカー)" }
        else if (!hasNR && isCa && !isEndcAnchor) { iconText = "4G+"; titleBase = "4G CA" }
        else if (!hasNR && !isCa && isEndcAnchor) { iconText = if (displayMode == 1) "4G/eLTE" else "4G"; titleBase = "4G(アンカー)" }
        else { iconText = "4G"; titleBase = "4G" }

        val titleShort = if (isEmergency) titleBase else "$titleBase - $titleSuffixShort$speedStrShort"
        val titleLong = if (isEmergency) titleBase else "$titleBase - $titleSuffixLong$speedStrLong"

        val iconParts = iconText.split("/")
        val topText = iconParts[0]
        val bottomText = if (iconParts.size > 1) iconParts[1] else ""

        val bodyBuilder = java.lang.StringBuilder()
        val sortedActiveChannels = activeChannels.sortedWith(compareBy({ !it.isPrimary }, { it.type == "4G" }))

        // 設定の読み込み
        val prefBandDisp = prefs.getInt("pref_show_band", 1)
        val showRsrp = prefs.getBoolean("pref_show_rsrp", true)
        val showRsrq = prefs.getBoolean("pref_show_rsrq", false)
        val showSinr = prefs.getBoolean("pref_show_sinr", false)
        val showCqi = prefs.getBoolean("pref_show_cqi", true)
        val showQl = prefs.getBoolean("pref_show_ql", true)
        val showCongestion = prefs.getBoolean("pref_show_congestion", false)
        val showTa = prefs.getBoolean("pref_show_ta", true)

        // 通知エリアに表示する詳細テキストを組み立てる
        sortedActiveChannels.forEach { ch ->
            val role = if (ch.isPrimary) "P" else "S"
            val prefix = if (ch.type == "5G") "n" else "B"
            val freqName = getFrequencyName(ch.band, ch.type == "5G")

            val bandName = when (prefBandDisp) {
                1 -> "$prefix${ch.band}"
                2 -> freqName.ifEmpty { "$prefix${ch.band}" }
                else -> "$prefix${ch.band}" + (if (freqName.isNotEmpty()) " $freqName" else "")
            }

            val bwStr = if (isEmergency) "" else if (ch.bwMhz > 0) "(${ch.bwMhz}M)" else "(不明)"

            val valList = mutableListOf<String>()
            if (showRsrp && ch.rsrp != UNAVAILABLE_VALUE) valList.add("${ch.rsrp}dBm")
            if (showRsrq && ch.rsrq != UNAVAILABLE_VALUE) valList.add("${ch.rsrq}dB")
            if (showSinr) valList.add("S:" + if (ch.sinr != UNAVAILABLE_VALUE) ch.sinr.toString() else "-")
            if (showCqi) valList.add("C:" + if (ch.cqi != UNAVAILABLE_VALUE && ch.cqi != 0) ch.cqi.toString() else "-")

            val valStr = if (valList.isNotEmpty()) "[${valList.joinToString(", ")}]" else ""
            val qlStr = if (showQl) " QL${ch.ql}" else ""
            val warning = if (showQl && ch.ql <= 3 && ch.rsrp != UNAVAILABLE_VALUE) "⚠" else ""

            val combinedVals = "$valStr$qlStr$warning".trim()

            // 1〜4がすべてオフならカッコ自体を消す
            val showAnyVal = showRsrp || showRsrq || showSinr || showCqi
            val finalVals = if (combinedVals.isNotEmpty()) combinedVals else if (showAnyVal) "[-]" else ""

            val expectRsrq = getExpectedRsrq(ch.rsrp, ch.type == "5G")
            val conSymbol = if (showCongestion) " " + getCongestionSymbol(ch.rsrq, expectRsrq) else ""

            val realFreqStr = getFrequencyFromArfcn(ch.band, ch.earfcn, ch.type == "5G")
            val freqMhz = realFreqStr.replace(" MHz", "").toDoubleOrNull() ?: 2000.0

            val distanceStr = if (showTa) calculateDistance(ch, txPowerIdx, envIdx, freqMhz) else ""

            val part1 = "$bandName$bwStr"
            val distAndCong = "$distanceStr$conSymbol".trim()

            val contentLines = listOf(part1, finalVals, distAndCong).filter { it.isNotEmpty() }.joinToString(" / ")
            val line = "${ch.type}[$role]: $contentLines\n"

            bodyBuilder.append(line)
        }

        val overlayTitle = if (isEmergency) "🚨 緊急通報専用モード" else "$titleBase $titleSuffixShort$speedStrShort$finalAlertStr"

        // UI（通知、オーバーレイ、Activity）の更新を実行
        forceUpdateNotificationData(topText, bottomText, titleShort, bodyBuilder.toString().trimEnd())
        updateOverlay(overlayTitle, if(isEmergency) "" else pingDisplayStr, sortedActiveChannels)

        sendUiUpdateIntent(channels, titleLong, dumpsysRaw, standardCells, dumpsysPool, isEmergency)

        // CSVファイルへの書き出し
        val cellsToLog = if (isCsvLogAllCells) {
            channels.sortedWith(compareBy({ !it.isRegistered }, { !it.isPrimary }, { it.type == "4G" }))
        } else {
            activeChannels.firstOrNull { it.isPrimary }?.let { listOf(it) } ?: emptyList()
        }
        appendCsvLog(cellsToLog, currentLat, currentLon)
    }

    // 解析したデータをJSON化してMainActivityに送信する
    private fun sendUiUpdateIntent(channels: List<ChannelData>, titleLong: String, dumpsysRaw: String, standardCells: List<ParsedCell>, dumpsysPool: List<DumpsysCell>, isEmergency: Boolean = false) {
        val prefs = getSharedPreferences("FiveGCheckerPrefs", Context.MODE_PRIVATE)
        val txPowerIdx = prefs.getInt("tx_power", 1)
        val envIdx = prefs.getInt("env_mode", 1)

        try {
            val jsonArray = org.json.JSONArray()
            val allSortedChannels = channels.sortedWith(compareBy({ !it.isRegistered }, { !it.isPrimary }, { it.type == "4G" }))
            allSortedChannels.forEach { ch ->
                val obj = org.json.JSONObject()
                val roleStr = if (ch.isRegistered) (if (ch.isPrimary) "P" else "S") else "N"
                obj.put("role", roleStr)
                obj.put("type", ch.type)
                obj.put("band", ch.band)
                obj.put("bw", ch.bwMhz)
                obj.put("arfcn", ch.earfcn)
                obj.put("pci", ch.pci)
                obj.put("rsrp", ch.rsrp)
                obj.put("rsrq", ch.rsrq)
                obj.put("sinr", ch.sinr)
                obj.put("rsrpLv", getSignalLevel(ch.rsrp))
                obj.put("rsrqLv", getRsrqLevel(ch.rsrq, ch.type == "5G"))
                obj.put("ql", ch.ql)

                val realFreqStr = getFrequencyFromArfcn(ch.band, ch.earfcn, ch.type == "5G")
                val freqMhz = realFreqStr.replace(" MHz", "").toDoubleOrNull() ?: 2000.0

                obj.put("distance", calculateDistance(ch, txPowerIdx, envIdx, freqMhz))
                obj.put("realFreq", realFreqStr)
                obj.put("cqi", ch.cqi)
                obj.put("ta", ch.ta)
                obj.put("ci", ch.ci)
                obj.put("carrier", getCarrierName(ch.mcc, ch.mnc, isEmergency))
                obj.put("isDuplicate", ch.isDuplicate)

                val expectRsrq = getExpectedRsrq(ch.rsrp, ch.type == "5G")
                obj.put("expectedRsrq", expectRsrq)
                obj.put("congestionSymbol", getCongestionSymbol(ch.rsrq, expectRsrq))
                obj.put("congestionText", getCongestionText(ch.rsrq, expectRsrq))
                obj.put("isEmergency", isEmergency)

                jsonArray.put(obj)
            }

            val statsArray = org.json.JSONArray()
            val sortedStats = bandStats.values.sortedByDescending { it.pTimeMs + it.sTimeMs }
            sortedStats.forEach { stat ->
                val obj = org.json.JSONObject()
                obj.put("type", stat.type)
                obj.put("band", stat.band)
                obj.put("freq", stat.freq)
                obj.put("pTime", stat.pTimeMs)
                obj.put("sTime", stat.sTimeMs)
                obj.put("total", stat.pTimeMs + stat.sTimeMs)
                statsArray.put(obj)
            }

            val historyArray = org.json.JSONArray()
            historyList.forEach { h ->
                val obj = org.json.JSONObject()
                obj.put("time", h.time)
                obj.put("msg", h.msg)
                obj.put("color", h.colorHex)
                historyArray.put(obj)
            }

            val updateIntent = Intent("UPDATE_UI_DATA")
            updateIntent.setPackage(packageName)
            updateIntent.putExtra("summary", titleLong)
            updateIntent.putExtra("cells_json", jsonArray.toString())
            updateIntent.putExtra("stats_json", statsArray.toString())
            updateIntent.putExtra("history_json", historyArray.toString())
            sendBroadcast(updateIntent)

        } catch (e: Exception) {
            logErrorToFile(e, "sendUiUpdateIntent")
        }

        // デバッグログ用のテキスト保存
        if (dumpsysRaw.isNotEmpty()) {
            val debugText = java.lang.StringBuilder()
            debugText.append("=== 5G CAT-RAT Debug Log ===\nTime: ${java.util.Date()}\n\n")

            if (prefs.getInt("ping_freq", 0) > 0) {
                val targetIdx = prefs.getInt("ping_target", 0)
                val ip = when (targetIdx) {
                    0 -> "8.8.8.8"; 1 -> "1.1.1.1";
                    else -> prefs.getString("ping_custom_ip", "8.8.8.8").takeIf { !it.isNullOrBlank() } ?: "8.8.8.8"
                }
                debugText.append("【0. Ping結果】\nTarget IP: $ip\n")
                if (isPingTimeout) debugText.append("Result: Timeout (100% Loss)\n\n")
                else if (currentPingMs >= 0) debugText.append("Result: ${currentPingMs} ms\n\n")
                else debugText.append("Result: 測定待ち...\n\n")
            }

            debugText.append("【1. 最終出力 (Channels)】\n")
            channels.sortedWith(compareBy({ !it.isRegistered }, { !it.isPrimary }, { it.type == "4G" })).forEach { debugText.append("$it\n") }

            debugText.append("\n【2. 統計情報 (Stats)】\n")
            if (bandStats.isEmpty()) {
                debugText.append("データなし\n")
            } else {
                val sortedStats = bandStats.values.sortedByDescending { it.pTimeMs + it.sTimeMs }
                sortedStats.forEachIndexed { index, stat ->
                    val pMin = stat.pTimeMs / 60000; val pSec = (stat.pTimeMs % 60000) / 1000
                    val sMin = stat.sTimeMs / 60000; val sSec = (stat.sTimeMs % 60000) / 1000
                    val totalMs = stat.pTimeMs + stat.sTimeMs
                    val tMin = totalMs / 60000; val tSec = (totalMs % 60000) / 1000
                    val prefix = if (stat.type.contains("5G")) "n" else "B"
                    debugText.append("${index + 1}位: $prefix${stat.band} ${stat.freq} (${stat.type}) | [P] ${pMin}分${String.format("%02d", pSec)}秒 [S] ${sMin}分${String.format("%02d", sSec)}秒 (計 ${tMin}分${String.format("%02d", tSec)}秒)\n")
                }
            }

            debugText.append("\n【3. タイムライン履歴 (History)】\n")
            if (historyList.isEmpty()) {
                debugText.append("履歴なし\n")
            } else {
                historyList.forEach { h ->
                    debugText.append("[${h.time}] ${h.msg}\n")
                }
            }

            debugText.append("\n【4. 標準API】\n")
            standardCells.forEach { debugText.append("$it\n") }
            debugText.append("\n【5. Dumpsys Pool】\n")
            dumpsysPool.forEach { debugText.append("Used:${it.used} | $it\n") }
            debugText.append("\n【6. 生ダンプ全文】\n")

            val cutIndex = dumpsysRaw.indexOf("listen logs:")
            val trimmedDumpsys = if (cutIndex != -1) {
                dumpsysRaw.substring(0, cutIndex) + "\n...[以降の不要な履歴ログはカットしました]..."
            } else if (dumpsysRaw.length > 60000) {
                dumpsysRaw.substring(0, 60000) + "\n...[長すぎるためカットしました]..."
            } else {
                dumpsysRaw
            }
            debugText.append(trimmedDumpsys)

            try { java.io.File(cacheDir, "debug_log.txt").writeText(debugText.toString()) } catch (e: Exception) {}
            try { java.io.File(cacheDir, "raw_dumpsys.txt").writeText(dumpsysRaw) } catch (e: Exception) {}
        }
    }

    // 品質スコア（QL）算出のための正規化メソッド（数値を0.0〜1.0の割合にする）
    private fun normalize(value: Double, min: Double, max: Double): Double {
        return when {
            value <= min -> 0.0
            value >= max -> 1.0
            else -> (value - min) / (max - min)
        }
    }

    // 電波強度や通信品質などから、アプリ独自の品質スコア（QL：0〜10）と理論通信速度を算出する
    private fun calculateQualityAndSpeed(ch: ChannelData) {
        val isMmWave = ch.type == "5G" && ch.band in listOf("257", "258", "259", "260", "261")
        val is5G = ch.type == "5G"
        val is4G = ch.type == "4G"

        val theoreticalMaxPerMhz = if (is5G) 12.0 else 7.5

        val hasRsrpRsrq = ch.rsrp != UNAVAILABLE_VALUE && ch.rsrq != UNAVAILABLE_VALUE && ch.rsrp <= 0 && ch.rsrq <= 0
        val hasSinr = ch.sinr != UNAVAILABLE_VALUE
        val hasCqi = ch.cqi != UNAVAILABLE_VALUE && ch.cqi in 0..15

        var qf = 0.0
        var overhead = 0.85

        if (!hasRsrpRsrq) {
            overhead = 0.75
            qf = min(getSignalLevel(ch.rsrp), getRsrqLevel(ch.rsrq, is5G)) / 10.0
        } else if (hasSinr && hasCqi) {
            overhead = 0.85
            if (is4G) {
                val nSinr = normalize(ch.sinr.toDouble(), 0.0, 30.0)
                val nCqi = normalize(ch.cqi.toDouble(), 0.0, 15.0)
                val nRsrq = normalize(ch.rsrq.toDouble(), -20.0, -3.0)
                val rsrpLoss = if (ch.rsrp < -100) normalize(ch.rsrp.toDouble(), -115.0, -100.0) else 1.0
                qf = (nCqi * 0.7 + nSinr * 0.3) * nRsrq * rsrpLoss
            } else if (is5G && !isMmWave) {
                val nSinr = normalize(ch.sinr.toDouble(), 0.0, 35.0)
                val nCqi = normalize(ch.cqi.toDouble(), 0.0, 15.0)
                val nRsrq = normalize(ch.rsrq.toDouble(), -24.0, -10.0)
                val rsrpLoss = if (ch.rsrp < -105) normalize(ch.rsrp.toDouble(), -120.0, -105.0) else 1.0
                qf = (nCqi * 0.7 + nSinr * 0.3) * nRsrq * rsrpLoss
            } else if (isMmWave) {
                val nSinr = normalize(ch.sinr.toDouble(), 0.0, 40.0)
                val nCqi = normalize(ch.cqi.toDouble(), 0.0, 15.0)
                val nRsrqRaw = normalize(ch.rsrq.toDouble(), -20.0, -12.0)
                val nRsrq = Math.pow(nRsrqRaw, 2.0)
                val rsrpLoss = if (ch.rsrp < -110) normalize(ch.rsrp.toDouble(), -125.0, -110.0) else 1.0
                qf = (nCqi * 0.6 + nSinr * 0.4) * nRsrq * rsrpLoss
            }
        } else if (hasSinr && !hasCqi) {
            overhead = 0.82
            if (is4G) {
                val nSinr = normalize(ch.sinr.toDouble(), 0.0, 30.0)
                val nRsrp = normalize(ch.rsrp.toDouble(), -110.0, -70.0)
                val baseQuality = (nSinr * 0.9 + nRsrp * 0.1)
                val congestionMultiplier = normalize(ch.rsrq.toDouble(), -20.0, -3.0)
                qf = baseQuality * congestionMultiplier
            } else if (is5G && !isMmWave) {
                val nSinr = normalize(ch.sinr.toDouble(), 0.0, 35.0)
                val nRsrp = normalize(ch.rsrp.toDouble(), -115.0, -75.0)
                val baseQuality = (nSinr * 0.85 + nRsrp * 0.15)
                val congestionMultiplier = normalize(ch.rsrq.toDouble(), -24.0, -10.0)
                qf = baseQuality * congestionMultiplier
            } else if (isMmWave) {
                val nSinr = normalize(ch.sinr.toDouble(), 0.0, 40.0)
                val nRsrp = normalize(ch.rsrp.toDouble(), -115.0, -90.0)
                val baseQuality = (nSinr * 0.75 + nRsrp * 0.25)
                val nRsrqRaw = normalize(ch.rsrq.toDouble(), -18.0, -11.0)
                val congestionMultiplier = Math.pow(nRsrqRaw, 2.0)
                qf = baseQuality * congestionMultiplier
            }
        } else {
            overhead = 0.80
            if (is4G) {
                val nRsrq = normalize(ch.rsrq.toDouble(), -19.0, -5.0)
                val nRsrp = normalize(ch.rsrp.toDouble(), -110.0, -70.0)
                qf = (nRsrq * 0.8 + nRsrp * 0.2)
            } else if (is5G && !isMmWave) {
                val nRsrq = normalize(ch.rsrq.toDouble(), -22.0, -10.0)
                val nRsrp = normalize(ch.rsrp.toDouble(), -115.0, -75.0)
                qf = (nRsrq * 0.75 + nRsrp * 0.25)
            } else if (isMmWave) {
                val nRsrqRaw = normalize(ch.rsrq.toDouble(), -18.0, -11.0)
                val nRsrq = Math.pow(nRsrqRaw, 2.5)
                val nRsrp = normalize(ch.rsrp.toDouble(), -115.0, -90.0)
                qf = (nRsrq * 0.6 + nRsrp * 0.4)
            }
        }

        ch.ql = Math.round(qf * 10.0).toInt().coerceIn(0, 10)
        ch.estimatedMbps = if (ch.bwMhz > 0) (ch.bwMhz * qf * theoreticalMaxPerMhz * overhead) else 0.0
    }

    private fun getSignalLevel(rsrp: Int): Int {
        if (rsrp == UNAVAILABLE_VALUE || rsrp > 0) return 0
        return when {
            rsrp >= -75 -> 10; rsrp >= -80 -> 9; rsrp >= -85 -> 8; rsrp >= -90 -> 7
            rsrp >= -95 -> 6; rsrp >= -100 -> 5; rsrp >= -105 -> 4; rsrp >= -110 -> 3
            rsrp >= -115 -> 2; rsrp >= -119 -> 1; else -> 0
        }
    }

    private fun getRsrqLevel(rsrq: Int, isNr: Boolean): Int {
        if (rsrq == UNAVAILABLE_VALUE || rsrq > 0) return 10
        return if (isNr) {
            when {
                rsrq >= -11 -> 10; rsrq == -12 -> 9; rsrq == -13 -> 8; rsrq == -14 -> 8
                rsrq == -15 -> 7; rsrq == -16 -> 6; rsrq == -17 -> 3; rsrq == -18 -> 1; else -> 0
            }
        } else {
            when {
                rsrq >= -9 -> 10; rsrq == -10 -> 9; rsrq == -11 -> 8; rsrq == -12 -> 8
                rsrq == -13 -> 7; rsrq == -14 -> 6; rsrq == -15 -> 3; rsrq == -16 -> 1; else -> 0
            }
        }
    }

    // 取得したセル情報をCSVファイルとして保存する
    private fun appendCsvLog(cells: List<ChannelData>, lat: Double, lon: Double) {
        if (!isCsvLoggingEnabled || cells.isEmpty()) return

        try {
            val prefs = getSharedPreferences("FiveGCheckerPrefs", Context.MODE_PRIVATE)
            val customPath = prefs.getString("csv_save_path", "")

            val timeStr = java.text.SimpleDateFormat("yyyy/MM/dd HH:mm:ss", java.util.Locale.getDefault()).format(java.util.Date())
            val builder = java.lang.StringBuilder()

            for (ch in cells) {
                val roleStr = if (ch.isRegistered) (if (ch.isPrimary) "P" else "S") else "N"
                val prefix = if (ch.type == "5G") "n" else "B"
                val line = "$timeStr,$lat,$lon,$roleStr,${ch.type},$prefix${ch.band},${ch.bwMhz},${ch.earfcn},${ch.pci},${ch.rsrp},${ch.rsrq},${ch.sinr},${ch.cqi},${ch.ta},${ch.ql}\n"
                builder.append(line)
            }

            if (customPath.isNullOrEmpty() || !customPath.startsWith("content://")) {
                // 従来の標準パス (Downloads/5GCATRAT/CSV) に保存
                val dir = java.io.File(android.os.Environment.getExternalStoragePublicDirectory(android.os.Environment.DIRECTORY_DOWNLOADS), "5GCATRAT/CSV")
                if (!dir.exists()) dir.mkdirs()

                val file = java.io.File(dir, "5GCATRAT_MapLog.csv")
                val isNewFile = !file.exists()
                if (isNewFile) file.appendText("Time,Lat,Lon,Role,Type,Band,BW,EARFCN,PCI,RSRP,RSRQ,SINR,CQI,TA,QL\n")

                file.appendText(builder.toString())
            } else {
                // ユーザーが選択したフォルダ (SAF方式) に保存
                val treeUri = android.net.Uri.parse(customPath)
                val tree = androidx.documentfile.provider.DocumentFile.fromTreeUri(this, treeUri)

                if (tree != null && tree.exists()) {
                    var file = tree.findFile("5GCATRAT_MapLog.csv")
                    var isNewFile = false

                    if (file == null) {
                        file = tree.createFile("text/csv", "5GCATRAT_MapLog.csv")
                        isNewFile = true
                    }

                    if (file != null) {
                        // "wa" = write + append (追記モードで開く)
                        contentResolver.openOutputStream(file.uri, "wa")?.use { outputStream ->
                            if (isNewFile) {
                                val header = "Time,Lat,Lon,Role,Type,Band,BW,EARFCN,PCI,RSRP,RSRQ,SINR,CQI,TA,QL\n"
                                outputStream.write(header.toByteArray())
                            }
                            outputStream.write(builder.toString().toByteArray())
                        }
                    }
                }
            }
        } catch (e: Exception) {
            logErrorToFile(e, "appendCsvLog_write")
        }
    }

    // 周波数番号(ARFCN)からバンド名を推測する処理
    private fun guessBandFromArfcn(arfcn: Int, isNr: Boolean, mcc: Int = UNAVAILABLE_VALUE, mnc: Int = UNAVAILABLE_VALUE): String {
        if (arfcn <= 0 || arfcn == UNAVAILABLE_VALUE) return "?"
        return if (isNr) {
            when (arfcn) {
                in 151600..160600 -> "28"
                in 340000..376000 -> "3"
                in 620000..680000 -> if (mcc == 440 && mnc == 10) "78" else "77" //ドコモならn78、それ以外ならn77
                in 693334..733333 -> "79"
                in 2054166..2104165 -> "257"
                else -> "?"
            }
        } else {
            when (arfcn) {
                in 0..599 -> "1"
                in 1200..1949 -> "3"
                in 3450..3799 -> "8"
                in 4750..4949 -> "11"
                in 5850..5999 -> "18"
                in 6000..6149 -> "19"
                in 6450..6599 -> "21"
                in 8690..9869 -> "28"
                in 39650..41589 -> "41"
                in 41590..43589 -> "42"
                else -> "?"
            }
        }
    }

    // 周波数番号(ARFCN)から実際の周波数(MHz)を計算する処理
    private fun getFrequencyFromArfcn(band: String, arfcn: Int, isNr: Boolean): String {
        if (arfcn <= 0 || arfcn == UNAVAILABLE_VALUE) return "不明"
        val cleanBand = band.replace(Regex("[^0-9]"), "")

        if (isNr) {
            val freq = when {
                arfcn in 0..599999 -> arfcn / 200.0
                arfcn in 600000..2016666 -> 3000.0 + (arfcn - 600000) * 0.015
                arfcn in 2016667..3279165 -> 24250.0 + (arfcn - 2016667) * 0.06
                else -> 0.0
            }
            return if (freq > 0) String.format("%.1f MHz", freq) else "不明"
        } else {
            val freq = when (cleanBand) {
                "1" -> 2110.0 + 0.1 * (arfcn - 0)
                "3" -> 1805.0 + 0.1 * (arfcn - 1200)
                "8" -> 925.0 + 0.1 * (arfcn - 3450)
                "11" -> 1475.9 + 0.1 * (arfcn - 4750)
                "18" -> 860.0 + 0.1 * (arfcn - 5850)
                "19" -> 875.0 + 0.1 * (arfcn - 6000)
                "21" -> 1495.9 + 0.1 * (arfcn - 6450)
                "28" -> 758.0 + 0.1 * (arfcn - 9210)
                "41" -> 2496.0 + 0.1 * (arfcn - 39650)
                "42" -> 3400.0 + 0.1 * (arfcn - 41590)
                else -> 0.0
            }
            return if (freq > 0) String.format("%.1f MHz", freq) else "不明"
        }
    }

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

    private fun getCarrierName(mcc: Int, mnc: Int, forceShow: Boolean = false): String {
        val simOperator = telephonyManager.simOperator
        val simMcc = simOperator?.take(3)?.toIntOrNull() ?: -1
        val simMnc = simOperator?.drop(3)?.toIntOrNull() ?: -1
        if (!forceShow && mcc == simMcc && mnc == simMnc) return ""
        if (mcc == 440 || mcc == 441) {
            return when (mnc) {
                10, 91 -> "(docomo)"; 20, 93 -> "(SoftBank)"; 50, 51, 52, 53, 54, 55, 92 -> "(KDDI)"
                0, 1 -> "(Y!mobile)"; 11, 94 -> "(Rakuten)"; else -> "(他社 PLMN:$mcc-$mnc)"
            }
        }
        return if (mcc != UNAVAILABLE_VALUE && mnc != UNAVAILABLE_VALUE) "(不明 PLMN:$mcc-$mnc)" else ""
    }

    // 画面上に常に表示されるオーバーレイ(小窓)の更新処理
    private fun updateOverlay(titleStr: String, pingStr: String, channels: List<ChannelData>) {
        handler.post {
            val prefs = getSharedPreferences("FiveGCheckerPrefs", Context.MODE_PRIVATE)
            val isEnabled = prefs.getBoolean("overlay_enabled", false)
            val showCongestion = prefs.getBoolean("pref_show_congestion", false)
            val showQl = prefs.getBoolean("pref_show_ql", true)

            if (!isEnabled || !Settings.canDrawOverlays(this)) {
                removeOverlay()
                return@post
            }

            if (windowManager == null) {
                windowManager = getSystemService(Context.WINDOW_SERVICE) as WindowManager
                overlayView = TextView(this).apply {
                    setTextColor(Color.WHITE)
                    setBackgroundColor(Color.parseColor("#80000000"))
                    setPadding(8, 2, 8, 2)
                    setShadowLayer(4f, 0f, 0f, Color.BLACK)
                }

                val params = WindowManager.LayoutParams(
                    WindowManager.LayoutParams.WRAP_CONTENT,
                    WindowManager.LayoutParams.WRAP_CONTENT,
                    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) WindowManager.LayoutParams.TYPE_APPLICATION_OVERLAY else WindowManager.LayoutParams.TYPE_PHONE,
                    WindowManager.LayoutParams.FLAG_NOT_FOCUSABLE or WindowManager.LayoutParams.FLAG_NOT_TOUCHABLE or WindowManager.LayoutParams.FLAG_LAYOUT_IN_SCREEN or WindowManager.LayoutParams.FLAG_LAYOUT_NO_LIMITS,
                    PixelFormat.TRANSLUCENT
                )

                val gravityPref = prefs.getInt("overlay_gravity", 0)
                params.gravity = when (gravityPref) {
                    1 -> Gravity.TOP or Gravity.CENTER_HORIZONTAL
                    2 -> Gravity.TOP or Gravity.END
                    else -> Gravity.TOP or Gravity.START
                }
                overlayView?.gravity = when (gravityPref) {
                    1 -> Gravity.CENTER
                    2 -> Gravity.END or Gravity.CENTER_VERTICAL
                    else -> Gravity.START or Gravity.CENTER_VERTICAL
                }

                windowManager?.addView(overlayView, params)
            } else {
                val params = overlayView?.layoutParams as WindowManager.LayoutParams
                val gravityPref = prefs.getInt("overlay_gravity", 0)
                params.gravity = when (gravityPref) {
                    1 -> Gravity.TOP or Gravity.CENTER_HORIZONTAL
                    2 -> Gravity.TOP or Gravity.END
                    else -> Gravity.TOP or Gravity.START
                }
                overlayView?.gravity = when (gravityPref) {
                    1 -> Gravity.CENTER
                    2 -> Gravity.END or Gravity.CENTER_VERTICAL
                    else -> Gravity.START or Gravity.CENTER_VERTICAL
                }
                windowManager?.updateViewLayout(overlayView, params)
            }

            val line2 = channels.joinToString(" ") { ch ->
                val role = if (ch.isPrimary) "P" else "S"
                val prefix = if (ch.type == "5G") "n" else "B"
                val expectRsrq = getExpectedRsrq(ch.rsrp, ch.type == "5G")
                val conSymbol = if (showCongestion) getCongestionSymbol(ch.rsrq, expectRsrq) else ""
                val qlStr = if (showQl) "(${ch.ql})" else ""
                "$role:$prefix${ch.band}$qlStr$conSymbol"
            }

            val br = if (pingStr.isNotEmpty() || line2.isNotEmpty()) "\n" else ""
            overlayView?.text = "$titleStr$br$pingStr$line2".trimEnd()
            overlayView?.textSize = prefs.getInt("overlay_size", 12).toFloat()

            val displayMetrics = resources.displayMetrics
            val xRatio = prefs.getInt("overlay_x", 500) / 1000f
            val yRatio = prefs.getInt("overlay_y", 0) / 1000f

            val params = overlayView?.layoutParams as WindowManager.LayoutParams
            params.x = (displayMetrics.widthPixels * xRatio).toInt()
            params.y = (displayMetrics.heightPixels * yRatio).toInt()
            windowManager?.updateViewLayout(overlayView, params)
        }
    }

    private fun removeOverlay() {
        if (overlayView != null) {
            windowManager?.removeView(overlayView)
            overlayView = null
            windowManager = null
        }
    }

    private fun getExpectedRsrq(rsrp: Int, isNr: Boolean): Int {
        if (rsrp == UNAVAILABLE_VALUE || rsrp > 0) return UNAVAILABLE_VALUE
        val absRsrp = Math.abs(rsrp)
        return if (isNr) -((absRsrp / 10) + 3) else -((absRsrp / 10) + 1)
    }

    private fun getCongestionSymbol(actual: Int, expect: Int): String {
        if (actual == UNAVAILABLE_VALUE || expect == UNAVAILABLE_VALUE) return "-"
        val diff = actual - expect
        return when {
            diff >= 1 -> "◎"
            diff >= -2 -> "○"
            diff >= -5 -> "△"
            else -> "×"
        }
    }

    private fun getCongestionText(actual: Int, expect: Int): String {
        if (actual == UNAVAILABLE_VALUE || expect == UNAVAILABLE_VALUE) return "-"
        val diff = actual - expect
        return when {
            diff >= 1 -> "◎空き"
            diff >= -2 -> "○普通"
            diff >= -5 -> "△混雑"
            else -> "×輻輳"
        }
    }

    // Timing Advance (TA) や RSRP (電波強度) から基地局までの推定距離を計算する
    private fun calculateDistance(ch: ChannelData, txPowerIdx: Int, envIdx: Int, freqMhz: Double): String {
        if (ch.ta != UNAVAILABLE_VALUE && ch.ta in 0..20000) {

            // 1. NSAのSCell（5GかつPrimaryではない）でTAが0の場合は未測定(サボり)とみなす
            if (ch.ta == 0 && ch.type == "5G" && !ch.isPrimary) {
                return "TA: - (未測定)"
            }

            // 2. SCSから1単位あたりの距離（m）を推測する
            val taMultiplier = when {
                ch.type == "5G" && ch.band in listOf("257", "258", "259", "260", "261") -> 9.75 // ミリ波 (SCS 120kHz)
                ch.type == "5G" && ch.bwMhz > 50 -> 39.06 // 50MHz超えのSub6はSCS 30kHzが確定的
                else -> 78.12 // 4Gや、15kHzの5G
            }

            if (ch.ta == 0) {
                return "TA0 <${taMultiplier.toInt()}m"
            } else {
                val taMeters = ch.ta * taMultiplier
                return if (taMeters < 1000.0) {
                    String.format("TA%d 約%dm", ch.ta, taMeters.toInt())
                } else {
                    String.format("TA%d 約%.2fkm", ch.ta, taMeters / 1000.0)
                }
            }
        }

        if (ch.rsrp == UNAVAILABLE_VALUE || ch.rsrp > 0) return "- m"

        val ptDbm = when (txPowerIdx) { 0 -> 46.0; 1 -> 30.0; 2 -> 15.0; else -> 30.0 }
        val n = when (envIdx) { 0 -> 2.5; 1 -> 3.5; 2 -> 4.0; 3 -> 5.0; else -> 3.5 }

        val safeFreq = if (freqMhz > 0.0) freqMhz else 2000.0
        val l0_1m = 20.0 * Math.log10(safeFreq) - 27.56

        val distanceLog = (ptDbm - ch.rsrp.toDouble() - l0_1m) / (10.0 * n)
        val distanceMeters = Math.pow(10.0, distanceLog).coerceAtLeast(1.0)

        return if (distanceMeters < 1000.0) {
            String.format("推 %dm", distanceMeters.toInt())
        } else {
            String.format("推 %.2fkm", distanceMeters / 1000.0)
        }
    }

    // ステータスバー通知の作成と更新
    private fun forceUpdateNotificationData(top: String, bottom: String, title: String, body: String) {
        currentTopText = top; currentBottomText = bottom; currentTitle = title; currentBody = body
        val manager = getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        manager.notify(NOTIFICATION_ID, createNotification())
    }

    private fun createNotification(): Notification {
        val customIcon = create2LineTextIcon(currentTopText, currentBottomText)
        val appIcon = Icon.createWithResource(this, R.mipmap.ic_launcher)
        val pendingUpdate = PendingIntent.getService(this, 0, Intent(this, NetworkMonitorService::class.java).apply { action = "ACTION_MANUAL_UPDATE" }, PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE)
        val pendingContent = PendingIntent.getActivity(this, 0, Intent(this, MainActivity::class.java).apply { flags = Intent.FLAG_ACTIVITY_SINGLE_TOP or Intent.FLAG_ACTIVITY_CLEAR_TOP }, PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE)

        return Notification.Builder(this, CHANNEL_ID)
            .setSmallIcon(customIcon).setLargeIcon(appIcon).setContentTitle(currentTitle).setContentText(currentBody).setStyle(Notification.BigTextStyle().bigText(currentBody))
            .setOngoing(true).setOnlyAlertOnce(true).setWhen(System.currentTimeMillis()).setContentIntent(pendingContent).setCategory(Notification.CATEGORY_SERVICE)
            .setGroup("NetworkMonitorGroup").setGroupAlertBehavior(Notification.GROUP_ALERT_SUMMARY).addAction(Notification.Action.Builder(null, "🔄 手動更新", pendingUpdate).build())
            .build()
    }

    private fun createNotificationChannel() {
        val channel = NotificationChannel(CHANNEL_ID, "通信モニター", NotificationManager.IMPORTANCE_DEFAULT).apply {
            description = "電波状態を常駐表示します"; enableLights(false); enableVibration(false); vibrationPattern = longArrayOf(0L); setSound(null, null); setShowBadge(false)
        }
        val manager = getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        manager.createNotificationChannel(channel)
    }

    // 通知バーに「5G」や「4G/CA」といった2行のテキストアイコンを動的生成する
    private fun create2LineTextIcon(topText: String, bottomText: String): Icon {
        val size = 128
        val bitmap = Bitmap.createBitmap(size, size, Bitmap.Config.ARGB_8888)
        val canvas = Canvas(bitmap)
        canvas.drawColor(Color.TRANSPARENT, PorterDuff.Mode.CLEAR)
        val paint = Paint().apply { color = Color.WHITE; isAntiAlias = true; typeface = Typeface.DEFAULT_BOLD; textSize = 100f }

        if (bottomText.isEmpty()) {
            val bounds = Rect(); paint.getTextBounds(topText, 0, topText.length, bounds)
            canvas.save(); canvas.scale(size.toFloat() / bounds.width(), size.toFloat() / bounds.height())
            canvas.drawText(topText, -bounds.left.toFloat(), -bounds.top.toFloat(), paint); canvas.restore()
        } else {
            val bT = Rect(); paint.getTextBounds(topText, 0, topText.length, bT)
            canvas.save(); canvas.scale(size.toFloat() / bT.width(), (size / 2f) / bT.height())
            canvas.drawText(topText, -bT.left.toFloat(), -bT.top.toFloat(), paint); canvas.restore()
            val bB = Rect(); paint.getTextBounds(bottomText, 0, bottomText.length, bB)
            canvas.save(); canvas.translate(0f, size / 2f); canvas.scale(size.toFloat() / bB.width(), (size / 2f) / bB.height())
            canvas.drawText(bottomText, -bB.left.toFloat(), -bB.top.toFloat(), paint); canvas.restore()
        }
        return Icon.createWithBitmap(bitmap)
    }
}