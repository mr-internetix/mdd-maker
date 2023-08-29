import docx2txt
import textract
from docx_utils.flatten import opc_to_flat_opc


def convert_to_text(path_for_docx):
    text_string = docx2txt.process(path_for_docx)
    # text = textract.process(path_for_docx)
    return text_string


def convert_to_xml(path_for_docx):
    opc_to_flat_opc(path_for_docx, "output.xml")
    return True


if __name__ == "__main__":
    pass
