import pandas as pd
import streamlit as st
import io
import os
from rapidfuzz import process, fuzz

# ─── Cấu hình cột chuẩn & alias ───────────────────────────────────────────────
COLUMN_ALIASES = {
    "Họ và tên":  ["họ và tên", "ho va ten", "họ tên", "ho ten", "tên", "ten", "full name", "name", "họ & tên"],
    "Lớp mới":    [],  # cột tạo tự động từ tên sheet, không map từ file
    "Giới tính":  ["giới tính", "gioi tinh", "gender", "sex"],
    "Ngày sinh":  ["ngày sinh", "ngay sinh", "ngày tháng năm sinh", "ngay thang nam sinh",
                   "dob", "date of birth", "birthday", "sinh ngày"],
    "Lớp":        ["lớp", "lop", "lớp học", "lop hoc", "class", "khối", "khoi"],
    "Địa chỉ":    ["địa chỉ", "dia chi", "address", "nơi ở", "noi o"],
    "Dân tộc":    ["dân tộc", "dan toc", "ethnicity"],
    "Mã HS":      ["mã hs", "ma hs", "mã học sinh", "ma hoc sinh", "student id", "id"],
    "Điện thoại": ["điện thoại", "dien thoai", "phone", "sđt", "sdt", "số điện thoại"],
    "Email":      ["email", "e-mail", "mail"],
}

PRIORITY_COLS = ["Họ và tên", "Lớp mới", "Giới tính", "Ngày sinh", "Lớp"]
ALL_STANDARD   = [k for k in COLUMN_ALIASES if k != "Lớp mới"]  # dùng để map từ file


def normalize(text: str) -> str:
    return str(text).strip().lower()


def map_column(col_name: str, threshold: int = 75):
    col_norm = normalize(col_name)
    for standard, aliases in COLUMN_ALIASES.items():
        if not aliases:
            continue
        if col_norm == normalize(standard) or col_norm in aliases:
            return standard
        result = process.extractOne(col_norm, aliases, scorer=fuzz.token_sort_ratio)
        if result and result[1] >= threshold:
            return standard
    return None


def detect_header_row(df_raw: pd.DataFrame, max_scan: int = 10) -> int:
    best_row, best_score = 0, 0
    for i in range(min(max_scan, len(df_raw))):
        score = sum(1 for v in df_raw.iloc[i] if map_column(str(v)) is not None)
        if score > best_score:
            best_score, best_row = score, i
    return best_row


def safe_str(series: pd.Series) -> pd.Series:
    """Ép Series bất kỳ kiểu về str, thay NaN/NaT bằng chuỗi rỗng."""
    return series.astype(str).replace({"nan": "", "NaT": "", "None": ""})


def fmt_date(val: str) -> str:
    if not val or val.strip() == "":
        return ""
    try:
        return pd.to_datetime(val, dayfirst=True).strftime("%d/%m/%Y")
    except Exception:
        return val


def read_sheet_smart(xl: pd.ExcelFile, sheet_name: str) -> pd.DataFrame:
    df_raw = xl.parse(sheet_name, header=None)
    header_row = detect_header_row(df_raw)
    df = xl.parse(sheet_name, header=header_row)

    # Chuẩn hoá tên cột gốc thành str
    raw_cols = [str(c).strip() for c in df.columns]

    # Map sang tên chuẩn; mỗi tên chuẩn chỉ được dùng 1 lần
    used: set[str] = set()
    new_cols: list[str] = []
    for col in raw_cols:
        std = map_column(col)
        if std and std not in used:
            new_cols.append(std)
            used.add(std)
        else:
            new_cols.append(col)  # giữ nguyên tên gốc nếu không map hoặc đã dùng

    df.columns = new_cols

    # Xử lý trùng tên (đặt hậu tố _2, _3, …)
    seen: dict[str, int] = {}
    final_cols: list[str] = []
    for c in df.columns:
        if c not in seen:
            seen[c] = 0
            final_cols.append(c)
        else:
            seen[c] += 1
            final_cols.append(f"{c}_{seen[c] + 1}")
    df.columns = final_cols

    # Gán tên sheet
    df["__sheet__"] = sheet_name

    # Bỏ hàng trống hoàn toàn
    df = df.dropna(how="all")

    # Ép TẤT CẢ cột về str ngay tại đây — loại bỏ mọi kiểu phức tạp
    for col in df.columns:
        df[col] = safe_str(df[col])

    return df


def merge_sheets(file_bytes) -> tuple[pd.DataFrame, list[str]]:
    xl = pd.ExcelFile(file_bytes)
    logs: list[str] = []
    frames: list[pd.DataFrame] = []

    for sheet in xl.sheet_names:
        try:
            df = read_sheet_smart(xl, sheet)
            logs.append(f"✅ Sheet **{sheet}**: {len(df)} dòng, cột → {list(df.columns)}")
            frames.append(df)
        except Exception as e:
            logs.append(f"⚠️ Sheet **{sheet}**: lỗi – {e}")

    if not frames:
        return pd.DataFrame(), logs

    # Concat — tất cả cột đã là str nên không có mixed-type
    merged = pd.concat(frames, ignore_index=True, sort=False)

    # Xử lý trùng tên cột sau concat (phòng trường hợp các sheet đặt tên khác nhau)
    seen: dict[str, int] = {}
    deduped: list[str] = []
    for c in merged.columns:
        if c not in seen:
            seen[c] = 0
            deduped.append(c)
        else:
            seen[c] += 1
            deduped.append(f"{c}_{seen[c] + 1}")
    merged.columns = deduped

    # Tạo cột "Lớp mới" từ tên sheet
    merged["Lớp mới"] = merged["__sheet__"] if "__sheet__" in merged.columns else ""

    # Đảm bảo "Giới tính" luôn tồn tại
    if "Giới tính" not in merged.columns:
        merged["Giới tính"] = ""

    # Định dạng Ngày sinh → dd/mm/yyyy
    if "Ngày sinh" in merged.columns:
        merged["Ngày sinh"] = merged["Ngày sinh"].apply(fmt_date)

    # Sắp xếp cột đầu ra
    present_priority = [c for c in PRIORITY_COLS if c in merged.columns]
    remaining_std    = [c for c in ALL_STANDARD if c not in PRIORITY_COLS and c in merged.columns]
    other_cols       = [c for c in merged.columns
                        if c not in PRIORITY_COLS and c not in ALL_STANDARD and c != "__sheet__"]
    merged = merged[present_priority + remaining_std + other_cols]

    # Đảm bảo không còn duplicate trước khi trả về
    assert merged.columns.duplicated().sum() == 0, \
        f"Vẫn còn cột trùng: {list(merged.columns[merged.columns.duplicated()])}"

    return merged, logs


# ─── Giao diện Streamlit ───────────────────────────────────────────────────────
st.set_page_config(page_title="Gộp Sheet Excel", page_icon="🐍", layout="centered")
st.title("🙃 Gộp nhiều Sheet trong 1 file Excel")
st.caption("Tự động nhận diện & chuẩn hoá cột: Họ và tên, Lớp mới, Giới tính, Ngày sinh, Lớp…")

uploaded = st.file_uploader("⬆️ Tải lên file Excel (.xlsx)", type=["xlsx", "xls"])

if uploaded:
    base_name   = os.path.splitext(uploaded.name)[0]
    output_name = f"{base_name}_đã gộp.xlsx"

    st.info(f"📂 File: **{uploaded.name}** — File đầu ra: **{output_name}**")

    with st.spinner("Đang xử lý…"):
        file_bytes        = uploaded.read()
        merged_df, logs   = merge_sheets(io.BytesIO(file_bytes))

    with st.expander("🔍 Chi tiết từng sheet", expanded=False):
        for log in logs:
            st.markdown(log)

    if merged_df.empty:
        st.error("Không đọc được dữ liệu từ file. Vui lòng kiểm tra lại.")
    else:
        st.success(f"✅ Gộp thành công **{len(merged_df)} dòng**, **{len(merged_df.columns)} cột**")
        st.dataframe(merged_df, use_container_width=True)

        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            merged_df.to_excel(writer, index=False, sheet_name="Đã gộp")
        buf.seek(0)

        st.download_button(
            label="⬇️ Tải về file đã gộp",
            data=buf,
            file_name=output_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
