# coding: UTF-8
"""
@software: PyCharm
@author: Lionel Johnson
@contact: https://fairy.host
@organization: https://github.com/FairylandFuture
@datetime: 2025-12-27 18:13:34 UTC+08:00
"""

import typing as t

from fairylandlogger import LogManager
from snownlp import SnowNLP
from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification
import torch


class SentimentAnalyzer:
    logger = LogManager.get_logger()

    def __init__(self, use_bert: bool = False):
        self.use_bert = use_bert

        if use_bert:
            model_name = "uer/roberta-base-finetuned-jd-binary-chinese"
            self.tokenizer = AutoTokenizer.from_pretrained(model_name)
            self.model = AutoModelForSequenceClassification.from_pretrained(model_name)
            self.pipeline = pipeline("text-classification", model=self.model, tokenizer=self.tokenizer)

    def __analyze_snownlp(self, text: str) -> float:
        if not text:
            return 0.5

        try:
            return SnowNLP(text).sentiments
        except Exception as error:
            self.logger.error(f"SnowNLP 情感分析失败: {error}")
            return 0.5

    def __analyze_bert(self, text: str) -> float:
        if not text:
            return 0.0

        try:
            inputs = self.tokenizer(text, return_tensors="pt", truncation=True, max_length=512)
            with torch.no_grad():
                outputs = self.model(**inputs)
            scores = torch.softmax(outputs.logits, dim=1).squeeze().tolist()
            positive_score = scores[1]
            return positive_score
        except Exception as error:
            self.logger.error(f"BERT 情感分析失败: {error}")
            return 0.5

    def analyze(self, text: str) -> float:
        if len(text.strip()) == 0:
            return 0.0
        elif len(text) > 512:
            text = text[:512]

        if self.use_bert:
            return self.__analyze_bert(text)

        return self.__analyze_snownlp(text)
