import sqlite3
import math
from flask import Flask, render_template, request, redirect, url_for
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
import shutil


app = Flask(__name__)

DB_DIR = os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "/Hedeflendirme/data")
DB_PATH = os.path.join(DB_DIR, "finans.db")

# klasör yoksa oluştur
os.makedirs(DB_DIR, exist_ok=True)

# ilk deploy ise local db'yi volume'e taşı
if not os.path.exists(DB_PATH):
    if os.path.exists("finans.db"):
        shutil.copy2("finans.db", DB_PATH)

# --- VERİTABANI YARDIMCILARI ---
def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row 
    return conn

def ayar_getir(proje_adi, anahtar, varsayilan=None):
    conn = get_db_connection()
    res = conn.execute("SELECT deger FROM ayarlar WHERE proje_adi = ? AND anahtar = ?", (proje_adi, anahtar)).fetchone()
    conn.close()
    return res["deger"] if res else varsayilan

def ayar_kaydet(proje_adi, anahtar, deger):
    conn = get_db_connection()
    conn.execute("INSERT OR REPLACE INTO ayarlar (proje_adi, anahtar, deger) VALUES (?, ?, ?)", 
                 (proje_adi, anahtar, str(deger)))
    conn.commit()
    conn.close()

# --- VERİ HESAPLAMA (DB ÜZERİNDEN) ---
def veri_hesapla_db():
    conn = get_db_connection()
    sorgu = '''
        SELECT 
            kurum_dosya_no as proje_adi,
            COUNT(*) as satir_sayisi,
            SUM(asil_alacak) as asil_toplam,
            SUM(kiyaslama) as kiyaslama_toplam,
            SUM(genel_toplam) as e_toplam,
            SUM(vekalet_ucreti) as g_toplam,
            SUM(tahsilat_toplami) as m_toplam,
            SUM(masraf) as n_toplam,
            SUM(takip_toplami) as t_toplam,
            MIN(takip_tarihi) as baslangic_min
        FROM projeler 
        GROUP BY kurum_dosya_no
    '''
    projeler_rows = conn.execute(sorgu).fetchall()
    conn.close()

    sonuc = []
    for row in projeler_rows:
        t_toplam = row["t_toplam"] if row["t_toplam"] else 0
        asil = row["asil_toplam"] or 0
        kiyas = row["kiyaslama_toplam"] or 0
        
        # --- GERÇEKLEŞME ORANI (KIYASLAMA / ASIL ALACAK) ---
        if asil > 0:
            g_orani = float(kiyas) / float(asil)
        else:
            g_orani = 0.0
            
        try:
            raw_date = str(row["baslangic_min"])
            baslangic = datetime.strptime(raw_date[:10], '%Y-%m-%d')
        except:
            baslangic = datetime.today()

        sonuc.append({
            "proje": row["proje_adi"],
            "takip_toplami": row["t_toplam"] or 0,
            "g_toplam": row["g_toplam"] or 0,
            "n_toplam": row["n_toplam"] or 0,
            "e_toplam": row["e_toplam"] or 0,
            "gerceklesme_orani": g_orani,
            "birim": int(t_toplam / row["satir_sayisi"]) if row["satir_sayisi"] > 0 else 0,
            "baslangic": baslangic
        })
    return sonuc

# --- HEDEF DEVİR MOTORU ---
def hedef_devir_uygula(aylar):
    # 1. Adım: Başlangıç değerlerini hazırla
    for ay in aylar:
        ay["orijinal_hedef"] = float(ay.get("hedef_raw", 0))
        ay["toplam_devir"] = 0.0 

    # 2. Adım: Satırları gez
    for i in range(len(aylar)):
        # Sadece tahsilat girişi yapılmış ayları baz al
        if aylar[i].get("tahsilat") != "" and aylar[i].get("tahsilat") is not None:
            try:
                tah_val = float(str(aylar[i]["tahsilat"]).replace(".", "").replace(",", "."))
            except:
                tah_val = 0
            
            # FARK: Hedef - Tahsilat
            # Örn: 500.000 - 600.000 = -100.000 TL
            fark = aylar[i]["orijinal_hedef"] - tah_val
            
            # FARK NE OLURSA OLSUN (Artı veya Eksi) Geleceğe Dağıt
            # Not: Eğer sadece başarısızlığı devretmek istersen buraya 'if fark > 0:' ekleyebilirsin.
            sonraki_aylar = aylar[i+1:]
            for j in range(len(sonraki_aylar)):
                next_ay = sonraki_aylar[j]
                oran_kat_sayisi = float(next_ay["oran"]) / 100
                
                # Eğer fark eksiyse (-100.000 * 0.09 = -9.000), hedefi düşürecektir.
                next_ay["toplam_devir"] += (fark * oran_kat_sayisi)

    # 3. Adım: Yeni hedefleri hesapla ve formatla
    for ay in aylar:
        # Nihai Hedef = İlk Hedef + (Geçmişten Gelen Artı veya Eksi Devirler)
        ay["hedef_raw"] = ay["orijinal_hedef"] + ay["toplam_devir"]
        
        # Eğer hedef eksiye düşerse (çok büyük tahsilat yapıldıysa) 0 göster
        if ay["hedef_raw"] < 0: ay["hedef_raw"] = 0
        
        ay["hedef"] = "{:,}".format(int(round(ay["hedef_raw"], 0))).replace(",", ".")
    
    return aylar

# --- ANA SAYFA ---
@app.route("/")
def index():
    tablo_ham = veri_hesapla_db()
    conn = get_db_connection()
    tablo_son = []
    bugun = datetime.now()

    for t in tablo_ham:
        p_adi = t["proje"]
        t_tahsilat_toplam = 0
        t_guncel_hedef_toplam = 0
        
        # 1. Projenin tüm aylarını detay sayfasındaki gibi simüle edelim
        aylar_simulasyon = []
        t_cursor = t["baslangic"]
        
        # Önce ham verileri topla
        for i in range(36):
            o_in = float(ayar_getir(p_adi, f"oran_{i}", 0.0))
            
            # Detay sayfasındaki orijinal hedef formülü
            ap = (t["takip_toplami"] * o_in) / 100
            v2 = int(ap * (i+1) * 9 / 100)
            vek = int(t["g_toplam"] * o_in / 100)
            mas = int(t["n_toplam"] * o_in / 100)
            
            m_h = ayar_getir(p_adi, f"hedef_{i}")
            h_raw = float(m_h) if m_h else float(math.ceil((ap + v2 + vek + mas) / 10000) * 10000)
            
            # Aylık tahsilatı çek
            ay_str = t_cursor.strftime('%Y-%m')
            ay_tah_res = conn.execute("SELECT SUM(tutar) FROM tahsilatlar WHERE kurum_dosya_no = ? AND strftime('%Y-%m', tarih) = ?", 
                                     (p_adi, ay_str)).fetchone()
            ay_tah = ay_tah_res[0] if ay_tah_res[0] else 0
            
            aylar_simulasyon.append({
                "index": i,
                "tarih_obj": t_cursor,
                "oran": o_in,
                "hedef_raw": h_raw,
                "tahsilat_raw": ay_tah,
                "tahsilat_str": str(ay_tah) if ay_tah > 0 else "" # Devir motoru için format
            })
            t_cursor += relativedelta(months=1)

        # 2. Detay sayfasındaki devir motorunu buraya da uygula (Birebir aynı mantık)
        gecmis_aylardan_gelen_toplam_devir = 0.0
        for i in range(36):
            curr = aylar_simulasyon[i]
            
            # Bu ayın nihai hedefi = Kendi hedefi + O ana kadar birikmiş devir payı
            # Önemli: Devir, ayın kendi oranıyla çarpılarak ekleniyor
            curr["final_hedef"] = curr["hedef_raw"] + (gecmis_aylardan_gelen_toplam_devir * (curr["oran"] / 100))
            
            # Eğer tahsilat girilmişse, bu aydan sonraki aylara devir farkı doğar
            if curr["tahsilat_str"] != "":
                fark = curr["hedef_raw"] - curr["tahsilat_raw"]
                gecmis_aylardan_gelen_toplam_devir += fark

        # 3. Toplamları Hesapla (Sadece bugüne kadar olan aylar için)
        son_durum_orani = "0%"
        for curr in aylar_simulasyon:
            t_tahsilat_toplam += curr["tahsilat_raw"]
            
            # Hedef toplamına sadece bugünün ayı ve öncesini dahil et
            if curr["tarih_obj"].year < bugun.year or (curr["tarih_obj"].year == bugun.year and curr["tarih_obj"].month <= bugun.month):
                t_guncel_hedef_toplam += curr["final_hedef"]
                
                # Durum Oranı Hesabı: (Gerçekleşen % - Planlanan Kumülatif %)
                # Bu hesaplama her ay güncellenerek en son (bugünkü) aya ulaşır
                planlanan_kumulatif = sum([a["oran"] for a in aylar_simulasyon[:curr["index"]+1]])
                durum_farki = round((t["gerceklesme_orani"] * 100) - planlanan_kumulatif, 2)
                son_durum_orani = f"%{durum_farki}"

        tablo_son.append({
            "proje": p_adi,
            "birim": t["birim"],
            "tahsilat": format(int(t_tahsilat_toplam), ",").replace(",", "."),
            "durum": format(int(t_tahsilat_toplam - t_guncel_hedef_toplam), ",").replace(",", "."),
            "durum_orani": son_durum_orani
        })

    conn.close()
    return render_template("index.html", tablo=tablo_son)

# --- DETAY SAYFASI ---
@app.route("/proje/<proje_adi>", methods=["GET", "POST"])
def proje_detay(proje_adi):
    proje_list = veri_hesapla_db()
    proje = next((x for x in proje_list if x["proje"] == proje_adi), None)
    if not proje: 
        return "Proje bulunamadı"

    if request.method == "POST":
        dagilim_agirliklari = [
            20, 15, 10, 5, 5, 5, 5, 4, 4, 4, 4, 4, 2, 2, 2, 0.5, 0.5, 0.5, 0.5, 0.5, 
            0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25
        ]
        yeni_toplam_oran_str = request.form.get("toplam_oran", "0").replace(",", ".")
        yeni_toplam_oran = float(yeni_toplam_oran_str)
        eski_toplam_oran = float(str(ayar_getir(proje_adi, "toplam_oran", "0")).replace(",", "."))

        if abs(yeni_toplam_oran - eski_toplam_oran) > 0.0001:
            ayar_kaydet(proje_adi, "toplam_oran", yeni_toplam_oran)
            for i in range(36):
                hesaplanan_pay = (yeni_toplam_oran * dagilim_agirliklari[i]) / 100
                ayar_kaydet(proje_adi, f"oran_{i}", round(hesaplanan_pay, 2))
                sorumlu_val = request.form.get(f"sorumlu_{i}", "")
                ayar_kaydet(proje_adi, f"sorumlu_{i}", sorumlu_val)
                #if sorumlu_val: ayar_kaydet(proje_adi, f"sorumlu_{i}", sorumlu_val)
        else:
            for i in range(36):
                ayar_kaydet(proje_adi, f"oran_{i}", request.form.get(f"oran_{i}", "0").replace(",", "."))
                sorumlu_val = request.form.get(f"sorumlu_{i}", "")
                ayar_kaydet(proje_adi, f"sorumlu_{i}", sorumlu_val)
                if sorumlu_val: ayar_kaydet(proje_adi, f"sorumlu_{i}", sorumlu_val)
                hedef_val = request.form.get(f"hedef_{i}")
                if hedef_val: ayar_kaydet(proje_adi, f"hedef_{i}", hedef_val.replace(".", ""))
            
        return redirect(url_for('proje_detay', proje_adi=proje_adi))

    conn = get_db_connection()
    son_t_row = conn.execute("SELECT MAX(tarih) FROM tahsilatlar WHERE kurum_dosya_no = ?", (proje_adi,)).fetchone()
    son_tarih = datetime.strptime(son_t_row[0], '%Y-%m-%d') if son_t_row and son_t_row[0] else None
    
    aylar = []
    t_cursor = proje["baslangic"]
    e_tah_toplam, e_hed_toplam, d_orani_dis = 0, 0, ""
    son_tahsilat_index = -1
    planlanan_oran_toplami = 0  # Son tahsilatlı aya kadar olan oranların toplamı
    tr_aylar = ["Ocak", "Şubat", "Mart", "Nisan", "Mayıs", "Haziran", 
             "Temmuz", "Ağustos", "Eylül", "Ekim", "Kasım", "Aralık"]

    for i in range(36):
        o_in = float(ayar_getir(proje_adi, f"oran_{i}", 0.0))
        
        # Hesaplamalar
        ap = (proje["takip_toplami"] * o_in) / 100
        v2 = int(ap * (i+1) * 9 / 100)
        vek = int(proje["g_toplam"] * o_in / 100)
        mas = int(proje["n_toplam"] * o_in / 100)
        
        m_h = ayar_getir(proje_adi, f"hedef_{i}")
        h_raw = float(m_h) if m_h else float(math.ceil((ap + v2 + vek + mas) / 10000) * 10000)
        
        # Aylık tahsilat
        ay_key = t_cursor.strftime('%Y-%m')
        ay_tah_res = conn.execute("SELECT SUM(tutar) FROM tahsilatlar WHERE kurum_dosya_no = ? AND strftime('%Y-%m', tarih) = ?", 
                                  (proje_adi, t_cursor.strftime('%Y-%m'))).fetchone()
        ay_tah = ay_tah_res[0] if ay_tah_res[0] else 0
        
        if ay_tah > 0:
            son_tahsilat_index = i

        e_tah_toplam += ay_tah
        if son_tarih and (t_cursor <= son_tarih):
            e_hed_toplam += h_raw

        # Sözlük yapısını hatasız oluşturma
        ay_verisi = {
            "tarih": f"{t_cursor.year} {tr_aylar[t_cursor.month - 1]}",
            "ana_para": format(int(ap), ",").replace(",", "."),
            "vade1": f"{(i+1)*9}%", 
            "vade2": format(v2, ",").replace(",", "."),
            "vekalet": format(vek, ",").replace(",", "."),
            "masraf": format(mas, ",").replace(",", "."),
            "avukat": format(int(ap+v2+vek+mas), ",").replace(",", "."),
            "oran": o_in,
            "sorumlu": ayar_getir(proje_adi, f"sorumlu_{i}", ""),
            "hedef_raw": (ap+v2+vek+mas), 
            "tahsilat": format(int(ay_tah), ",").replace(",", ".") if ay_tah > 0 else "",
            "gerceklesme_orani_satir": "",
            "durum_orani_satir": ""
        }
        aylar.append(ay_verisi)
        t_cursor += relativedelta(months=1)

    # --- SON SATIR HESAPLAMALARI ---
    panel_durum_orani = "0%" # Başlangıç değeri
    
    if son_tahsilat_index != -1:
        # 1. Gerçekleşme Oranı (Tam Sayı)
        g_oran_yuzde = int(round(proje["gerceklesme_orani"] * 100))
        aylar[son_tahsilat_index]["gerceklesme_orani_satir"] = f"%{g_oran_yuzde}"
        
        # 2. Durum Oranı (Fark)
        planlanan_kumulatif = sum([float(ayar_getir(proje_adi, f"oran_{j}", 0)) for j in range(son_tahsilat_index + 1)])
        durum_farki = int(round((proje["gerceklesme_orani"] * 100) - planlanan_kumulatif))
        
        deger_str = f"%{durum_farki}"
        aylar[son_tahsilat_index]["durum_orani_satir"] = deger_str
        panel_durum_orani = deger_str # Panel için bu değeri değişkene atıyoruz

    conn.close()
    aylar = hedef_devir_uygula(aylar)

    return render_template("proje_detay.html", 
                           proje_adi=proje_adi, aylar=aylar,
                           tahsilat=format(int(e_tah_toplam), ",").replace(",", ".") + " ₺",
                           durum=format(int(e_tah_toplam - e_hed_toplam), ",").replace(",", ".") + " ₺",
                           durum_orani=panel_durum_orani,
                           toplam_oran=ayar_getir(proje_adi, "toplam_oran", 0),
                           sorumlu_listesi=["BAHAR", "BENGİSU", "CANNUR", "ESRA QNB", "HARUN", "KÜBRA", "MELEK", "MERVE", "MUSTAFA", "NUR", "ÖZLEM", "RANA", "SİBEL / HAVVA", "SİNEM", "TEVFİK", "TUĞBA", "TUĞBA-2", "ZERRİN"],
                           birim_dosya=format(proje["birim"], ",").replace(",", "."),
                           aylik_dosya=format(int(proje["takip_toplami"]), ",").replace(",", "."),
                           vekalet_ucret=format(int(proje["g_toplam"]), ",").replace(",", "."),
                           toplam_masraf=format(int(proje["n_toplam"]), ",").replace(",", "."))

if __name__ == "__main__":
    app.run(debug=True)