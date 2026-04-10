## 5G CAT-RAT

5G/4Gのキャリアアグリゲーション（CA）状態や、基地局の物理パラメータをリアルタイムで可視化・ログ保存する解析ツールです。

## スクリーンショット
* 通知アイコン：実際の接続状況をリアルタイムに反映する通知アイコンRAT表示
* メイン画面（テキストタブ）：RSRP, RSRQ, SINR, CQI, TAなどの詳細一覧 。
* グラフ画面（積層表示）：各セルの帯域幅（MHz）と品質（QL）を視覚化した画面 。
<img width="270" height="601" alt="1_詳細画面" src="https://github.com/user-attachments/assets/af9871f6-776d-409b-b949-e47fbde83b63" />
<img width="270" height="601" alt="2_積層グラフ" src="https://github.com/user-attachments/assets/9cb65b05-2ba3-4af7-aa16-87eeb6a25e87" />

* 統計画面：どのバンドにどのくらい接続していたかの時間集計 。  
* 詳細設定：各パラメータの表示オンオフやCSV保存先選択の画面 。  
<img width="270" height="601" alt="3_統計画面" src="https://github.com/user-attachments/assets/e0769782-6fa8-4aee-8269-0d35c9daf4c0" />
<img width="270" height=570" alt="5_詳細設定1" src="https://github.com/user-attachments/assets/5f4c07cb-6aba-4e3e-8fa9-c839da55ebb0" />
<img width="270" height="378" alt="6_詳細設定2" src="https://github.com/user-attachments/assets/c44f357d-e494-4f2b-8eed-f2917f4b4444" />

* 通知ドロワー：通知カードでの詳細表示状態 。
<img width="540" height="215" alt="4_通知ドロワー" src="https://github.com/user-attachments/assets/c408f56d-7821-4bff-b3de-895f1b05b11b" />

## 動作要件
* OS：Android 11以上  
* 必須アプリ
  * Shizukuアプリが実行されていること。（dumpsys 取得に使用）  
* 必要な権限
  * 位置情報：基地局情報の詳細取得、およびCSVログへの座標記録に使用します。  
  * 電話 (端末の状態)：接続中のSIM情報や基地局情報の取得に使用します。  
  * 通知：通知ドロワーでのリアルタイム表示に使用します。  
  * 他のアプリの上に重ねて表示: オーバーレイ表示機能を使用する場合に必要です。  

## 主な機能
* 5G NSA/SA および 4G CA状態のリアルタイム判別。  
* Android標準APIでは取得困難な物理レイヤー情報の dumpsys 解析による補完。  
* 品質スコア（QL）の独自算出と理論上限速度の推定。  
* キャリアや周波数に基づくバンド名の動的推測。  
* Googleマップ等での解析に使えるかもしれない位置情報付きCSVログ保存 。  

## 免責事項
本アプリは開発者の個人利用および技術的興味を目的として作成されたものです。  
原則として、不具合修正の保証や個別のサポート、新機能のリクエストへの対応は行っておりません。  
解析結果の正確性については、端末やキャリアの挙動に依存するため保証いたしかねます。  
