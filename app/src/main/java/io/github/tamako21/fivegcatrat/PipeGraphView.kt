package io.github.tamako21.fivegcatrat

import android.content.Context
import android.graphics.Canvas
import android.graphics.Color
import android.graphics.Paint
import android.graphics.RectF
import android.util.AttributeSet
import android.view.View
import kotlin.math.max

// 帯域幅をパイプ（土管）のように視覚化するためのカスタムビュークラス
class PipeGraphView @JvmOverloads constructor(
    context: Context, attrs: AttributeSet? = null, defStyleAttr: Int = 0
) : View(context, attrs, defStyleAttr) {

    // グラフの描画に必要なデータをまとめたデータクラス
    data class CellGraphData(
        val type: String,             // "4G" や "5G"
        val role: String,             // "P" (PCell) や "S" (SCell)
        val band: String,             // 表示するバンド名
        val bwMhz: Int,               // 帯域幅（例: 20）
        val realFreq: String,         // 実際の中心周波数
        val valStr: String,           // 電波強度などの文字列
        val distance: String,         // 推定距離
        val congestionSymbol: String, // 混雑度の記号
        val ql: Int                   // 品質レベル(Quality Level)
    )

    // 外部から渡されるセルのリスト
    private var cells = listOf<CellGraphData>()

    // --- 描画用のペン（Paint）の初期化 ---
    private val mainGridPaint = Paint().apply { color = Color.parseColor("#666666"); isAntiAlias = true }
    private val subGridPaint = Paint().apply { color = Color.parseColor("#333333"); isAntiAlias = true }
    private val gridTextPaint = Paint().apply { color = Color.parseColor("#AAAAAA"); textAlign = Paint.Align.RIGHT; isAntiAlias = true }
    private val pipeLinePaint = Paint().apply { style = Paint.Style.STROKE; isAntiAlias = true } // 外枠用
    private val fillPaint = Paint().apply { style = Paint.Style.FILL; isAntiAlias = true }       // 塗りつぶし用
    private val infoTextPaint = Paint().apply {
        color = Color.WHITE
        textAlign = Paint.Align.CENTER
        isAntiAlias = true
        setShadowLayer(8f, 0f, 0f, Color.BLACK)
    }

    // ★変更: メモリリーク対策（onDraw内で新しいオブジェクトを作らないよう、外に用意して使い回す）
    private val pipeRect = RectF()

    // グラフの各セルを塗り分けるためのカラーパレット
    private val colors = intArrayOf(
        Color.parseColor("#2196F3"), Color.parseColor("#F44336"), Color.parseColor("#4CAF50"),
        Color.parseColor("#FF9800"), Color.parseColor("#9C27B0"), Color.parseColor("#00BCD4")
    )

    // 外部（MainActivityなど）からデータを受け取り、再描画(invalidate)を要求するメソッド
    fun setData(newCells: List<CellGraphData>) {
        this.cells = newCells
        invalidate()
    }

    // 実際の描画処理を行うコアメソッド
    override fun onDraw(canvas: Canvas) {
        super.onDraw(canvas)
        if (cells.isEmpty()) return

        val width = width.toFloat()
        val height = height.toFloat()
        val density = resources.displayMetrics.density

        // ペンの太さや文字サイズを画面のピクセル密度に合わせて調整
        gridTextPaint.textSize = 12f * density
        mainGridPaint.strokeWidth = 2f * density
        subGridPaint.strokeWidth = 1f * density
        val strokeW = 3f * density
        pipeLinePaint.strokeWidth = strokeW

        // グラフ全体の描画エリア幅を計算
        val startX = 50f * density
        val endX = width - (10f * density)
        val graphWidth = endX - startX

        // 補助線を見せるため、パイプ(枠)自体の幅を90%にするマージン(左右5%ずつ)を計算
        val margin = graphWidth * 0.05f
        val pipeStartX = startX + margin
        val pipeEndX = endX - margin

        // 上下の余白を設定
        val topMargin = 20f * density
        val bottomMargin = 20f * density
        val graphHeight = height - topMargin - bottomMargin
        val endY = height - bottomMargin

        // 全セルの帯域幅を合計し、グラフの最大スケール(Y軸の最大値)を計算
        val totalBw = cells.sumOf { it.bwMhz }
        val maxTick = max(100, ((totalBw + 49) / 50) * 50)
        val pxPerMhz = graphHeight / maxTick.toFloat()

        // 背景のグリッド線（目盛り）の描画
        for (tick in 0..maxTick step 10) {
            val y = endY - (tick * pxPerMhz)
            if (tick % 50 == 0) {
                // 50MHzごとの太い線とテキスト
                canvas.drawLine(startX, y, endX, y, mainGridPaint)
                canvas.drawText("${tick}M", startX - (8f * density), y + (4f * density), gridTextPaint)
            } else {
                // 10MHzごとの細い線
                canvas.drawLine(startX, y, endX, y, subGridPaint)
            }
        }

        // 塗りつぶし部分のX座標（枠線の内側）
        val fillStartX = pipeStartX + strokeW
        val fillEndX = pipeEndX - strokeW

        // 1. 実効帯域幅（品質レベルに応じた塗りつぶし）の描画
        var currentEffY = endY
        cells.forEachIndexed { index, cell ->
            // QL(品質)を掛けて実際に使えているであろう帯域幅を算出
            val effBw = cell.bwMhz * (cell.ql / 10f)
            val effHeight = effBw * pxPerMhz
            if (effHeight > 0) {
                val topY = currentEffY - effHeight
                val baseColor = colors[index % colors.size]
                fillPaint.color = Color.argb(180, Color.red(baseColor), Color.green(baseColor), Color.blue(baseColor))
                canvas.drawRect(fillStartX, topY, fillEndX, currentEffY, fillPaint)
                currentEffY = topY
            }
        }

        // 2. 物理帯域幅（外枠）とテキストの描画
        var currentPhysY = endY
        val halfStroke = strokeW / 2f

        cells.forEachIndexed { index, cell ->
            val physHeight = cell.bwMhz * pxPerMhz
            val topY = currentPhysY - physHeight
            val baseColor = colors[index % colors.size]

            pipeLinePaint.color = baseColor

            // ★変更: 毎回新しく作らず、用意しておいた箱(pipeRect)の数値を更新して使い回す
            pipeRect.set(pipeStartX + halfStroke, topY + halfStroke, pipeEndX - halfStroke, currentPhysY - halfStroke)
            canvas.drawRect(pipeRect, pipeLinePaint)

            // ★変更: ここからテキストのはみ出し対策ロジック
            val prefix = if (cell.type == "5G") "" else ""
            val freqStr = cell.realFreq.replace(" MHz", "MHz")

            // 短縮版と詳細版のテキストを用意
            val shortStr = "[${cell.role}] $prefix${cell.band} $freqStr ${cell.bwMhz}MHz"
            val qlText = if (cell.ql >= 0) "QL${cell.ql}" else ""
            var infoStr = "$shortStr ${cell.valStr} $qlText ${cell.distance} ${cell.congestionSymbol}".replace(Regex("\\s+"), " ").trim()

            // 一旦デフォルトの文字サイズで高さを計算
            infoTextPaint.textSize = 13f * density
            var textHeight = infoTextPaint.descent() - infoTextPaint.ascent()

            // 枠の高さに余裕がない場合の処理（文字サイズを動的に縮小する）
            if (physHeight < textHeight * 1.5f) {
                // まず文字サイズを少し小さくしてみる
                infoTextPaint.textSize = 10f * density
                textHeight = infoTextPaint.descent() - infoTextPaint.ascent()

                // それでも枠に対して文字が大きい（5MHzなど）場合は、短縮版のテキストに切り替える
                if (physHeight < textHeight * 1.2f) {
                    infoStr = shortStr
                    // 極端に狭い場合はさらに文字を小さくする
                    if (physHeight < textHeight) {
                        infoTextPaint.textSize = 8.5f * density
                        textHeight = infoTextPaint.descent() - infoTextPaint.ascent()
                    }
                }
            }

            // 上下の中央を計算して文字を描画
            val centerY = currentPhysY - (physHeight / 2f)
            val textY = centerY - ((infoTextPaint.descent() + infoTextPaint.ascent()) / 2f)

            canvas.drawText(infoStr, startX + (graphWidth / 2f), textY, infoTextPaint)

            // 次のセルのためにY座標を更新
            currentPhysY = topY
        }
    }
}