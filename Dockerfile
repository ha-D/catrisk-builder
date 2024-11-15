FROM coreoasis/model_worker:2.3.10

RUN pip3 install --break-system-packages msoffcrypto-tool openpyxl

ADD Catrisks/ /home/worker/model

USER root
RUN chown -R worker /home/worker/model/
USER worker

