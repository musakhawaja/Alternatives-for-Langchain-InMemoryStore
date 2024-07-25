from langchain.schema import Document
from langchain.text_splitter import TextSplitter
import re
from typing import List

class RegexTextSplitter(TextSplitter):
    def __init__(self, pattern: str):
        self.pattern = pattern

    def split_text(self, text: str) -> List[str]:
        matches = re.findall(self.pattern, text)
        split_texts = []
        for match in matches:
            split_texts.append(f"{match[0]}: {match[1]}")
        return split_texts

    def split_documents(self, documents: List[Document]) -> List[Document]:
        split_documents = []
        for document in documents:
            split_texts = self.split_text(document.page_content)
            for split_text in split_texts:
                split_documents.append(Document(page_content=split_text, metadata=document.metadata))
        return split_documents