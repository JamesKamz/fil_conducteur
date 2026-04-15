import os

from etl.transform.spark_processor import _fallback_process_gdrive_data


def test_fallback_process_gdrive_data_non_utf8(tmpdir):
    input_dir = tmpdir.mkdir("raw")
    output_dir = tmpdir.mkdir("processed")

    input_file = input_dir.join("gdrive_data.csv")
    # Include CP1252-specific byte to ensure utf-8 decode path fails first.
    content = "name,city\nAndr\xe9,Paris\n".encode("cp1252")
    input_file.write_binary(content)

    _fallback_process_gdrive_data(str(input_file), str(output_dir))

    out_file = os.path.join(str(output_dir), "gdrive_data", "part-00000.csv")
    assert os.path.exists(out_file)


def test_fallback_process_gdrive_data_missing_input(tmpdir):
    output_dir = tmpdir.mkdir("processed")

    _fallback_process_gdrive_data(
        os.path.join(str(tmpdir), "missing.csv"),
        str(output_dir),
    )

    assert not os.path.exists(os.path.join(str(output_dir), "gdrive_data"))
