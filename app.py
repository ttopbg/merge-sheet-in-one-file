import pandas as pd
import streamlit as st
import io
import os
from rapidfuzz import process, fuzz

# ─── Cấu hình cột chuẩn & alias ───────────────────────────────────────────────
COLUMN_ALIASES = {
    "Họ và tên":  ["họ và tên", "ho va ten", "họ tên", "ho ten", "tên", "ten", "full name", "name", "họ & tên"],
    "Lớp":        ["lớp", "lop", "lớp học", "lop hoc", "class", "khối", "khoi"],
    "Giới tính":  ["giới tính", "gioi tinh", "gender", "sex"],
    "Ngày sinh":  ["ngày sinh", "ngay sinh", "ngày tháng năm sinh", "ngay thang nam sinh",
                   "dob", "date of birth", "birthday", "sinh ngày"],
    "Địa chỉ":    ["địa chỉ", "dia chi", "address", "nơi ở", "noi o"],
    "Dân tộc":    ["dân tộc", "dan toc", "ethnicity"],
    "Mã HS":      ["mã hs", "ma hs", "mã học sinh", "ma hoc sinh", "student id", "id"],
    "Điện thoại": ["điện thoại", "dien thoai", "phone", "sđt", "sdt", "số điện thoại"],
    "Email":      ["email", "e-mail", "mail"],
}

# Thứ tự cột ưu tiên trong file output
PRIORITY_COLS = ["Họ và tên", "Lớp mới", "Lớp", "Giới tính", "Ngày sinh"]


def normalize(text: str) -> str:
    return str(text).strip().lower()


def map_column(col_name: str, threshold: int = 75):
    col_norm = normalize(col_name)
    for standard, aliases in COLUMN_ALIASES.items():
        if col_norm in aliases or col_norm == normalize(standard):
            return standard
        result = process.extractOne(col_norm, aliases, scorer=fuzz.token_sort_ratio)
        if result and result[1] >= threshold:
            return standard
    return None


def detect_header_row(df_raw: pd.DataFrame, max_scan: int = 10) -> int:
    best_row, best_score = 0, 0
    for i in range(min(max_scan, len(df_raw))):
        row = df_raw.iloc[i]
        score = sum(1 for v in row if map_column(str(v)) is not None)
        if score > best_score:
            best_score, best_row = score, i
    return best_row


def dedup_columns(cols):
    """Đổi tên cột trùng thành col, col_2, col_3, ..."""
    seen = {}
    result = []
    for c in cols:
        if c not in seen:
            seen[c] = 0
            result.append(c)
        else:
            seen[c] += 1
            result.append(f"{c}_{seen[c] + 1}")
    return result


def fmt_date(val):
    if pd.isna(val) or str(val).strip() in ("", "nan"):
        return ""
    if hasattr(val, "strftime"):
        return val.strftime("%d/%m/%Y")
    try:
        return pd.to_datetime(val, dayfirst=True).strftime("%d/%m/%Y")
    except Exception:
        return str(val)


def read_sheet_smart(xl: pd.ExcelFile, sheet_name: str) -> pd.DataFrame:
    df_raw = xl.parse(sheet_name, header=None)
    header_row = detect_header_row(df_raw)

    df = xl.parse(sheet_name, header=header_row)
    df.columns = [str(c).strip() for c in df.columns]

    # Đổi tên cột sang tên chuẩn (mỗi chuẩn chỉ map 1 lần, tránh trùng)
    rename_map = {}
    used_standards = set()
    for col in df.columns:
        standard = map_column(col)
        if standard and standard not in used_standards:
            rename_map[col] = standard
            used_standards.add(standard)

    df = df.rename(columns=rename_map)

    # Xử lý tên cột trùng sau rename
    df.columns = dedup_columns(list(df.columns))

    df["__sheet__"] = sheet_name
    df = df.dropna(how="all")

    # Ép object → str để tránh mixed-type lỗi PyArrow
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str).replace("nan", "")

    return df


def merge_sheets(file_bytes):
    xl = pd.ExcelFile(file_bytes)
    logs = []
    frames = []

    for sheet in xl.sheet_names:
        try:
            df = read_sheet_smart(xl, sheet)
            logs.append(f"✅ Sheet **{sheet}**: {len(df)} dòng, cột → {list(df.columns)}")
            frames.append(df)
        except Exception as e:
            logs.append(f"⚠️ Sheet **{sheet}**: lỗi – {e}")

    if not frames:
        return pd.DataFrame(), logs

    merged = pd.concat(frames, ignore_index=True, sort=False)

    # Ép lại object → str lần nữa sau concat (phòng mixed type)
    for col in merged.columns:
        if merged[col].dtype == object:
            merged[col] = merged[col].astype(str).replace("nan", "")

    # Định dạng Ngày sinh → dd/mm/yyyy
    if "Ngày sinh" in merged.columns:
        merged["Ngày sinh"] = merged["Ngày sinh"].apply(fmt_date)

    # Tạo cột "Lớp mới" từ tên sheet (lưu trong __sheet__)
    if "__sheet__" in merged.columns:
        merged["Lớp mới"] = merged["__sheet__"]
    else:
        merged["Lớp mới"] = ""

    # Đảm bảo cột Giới tính luôn tồn tại
    if "Giới tính" not in merged.columns:
        merged["Giới tính"] = ""

    # Sắp xếp cột: priority → chuẩn còn lại → cột khác
    all_standard = list(COLUMN_ALIASES.keys())
    present_priority = [c for c in PRIORITY_COLS if c in merged.columns]
    remaining_standard = [c for c in all_standard if c not in PRIORITY_COLS and c in merged.columns]
    other_cols = [c for c in merged.columns if c not in all_standard and c != "__sheet__"]

    final_order = present_priority + remaining_standard + other_cols
    if "__sheet__" in merged.columns:
        final_order.append("__sheet__")

    merged = merged[final_order]
    return merged, logs


# ─── Giao diện Streamlit ───────────────────────────────────────────────────────
st.set_page_config(page_title="Gộp Sheet Excel", page_icon="🐍", layout="centered")
st.title("🙃 Gộp nhiều Sheet trong 1 file Excel")
st.caption("Tự động nhận diện & chuẩn hoá cột: Họ và tên, Lớp, Giới tính, Ngày sinh…")

uploaded = st.file_uploader("⬆️ Tải lên file Excel (.xlsx)", type=["xlsx", "xls"])

if uploaded:
    base_name = os.path.splitext(uploaded.name)[0]
    output_name = f"{base_name}_đã gộp.xlsx"

    st.info(f"📂 File: **{uploaded.name}** — File đầu ra sẽ là: **{output_name}**")

    with st.spinner("Đang xử lý…"):
        file_bytes = uploaded.read()
        merged_df, logs = merge_sheets(io.BytesIO(file_bytes))

    with st.expander("🔍 Chi tiết từng sheet", expanded=False):
        for log in logs:
            st.markdown(log)

    if merged_df.empty:
        st.error("Không đọc được dữ liệu từ file. Vui lòng kiểm tra lại.")
    else:
        display_df = merged_df.drop(columns=["__sheet__"], errors="ignore")

        st.success(f"✅ Gộp thành công **{len(display_df)} dòng**, **{len(display_df.columns)} cột**")
        st.dataframe(display_df, use_container_width=True)

        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            display_df.to_excel(writer, index=False, sheet_name="Đã gộp")
        buf.seek(0)

        st.download_button(
            label="⬇️ Tải về file đã gộp",
            data=buf,
            file_name=output_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
