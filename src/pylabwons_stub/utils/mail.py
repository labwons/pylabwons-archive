from pylabwons import DataDict
from sib_api_v3_sdk.rest import ApiException
from typing import Callable, Dict, Union
import base64, os, sib_api_v3_sdk


class Mailing:

    def __init__(self, api:str = '', logger:Callable=print):

        self._client = sib_api_v3_sdk.Configuration()
        self._client.api_key['api-key'] = api
        self._sender = sib_api_v3_sdk.TransactionalEmailsApi(sib_api_v3_sdk.ApiClient(self._client))

        self._config = DataDict(
            sender=DataDict(
                name="no_reply",
                email="no_reply@labwons.com"
            ),
            to=DataDict(
                customer='jhlee_0319@naver.com'
            ),
            subject='',
            html_content='',
        )
        self.logger = logger
        return

    def __str__(self):
        return str(self._config)

    @property
    def content(self) -> str:
        return f"{self._config.html_content}<br><br><p style='color:gray;'>* 본 메일은 발신 전용으로 회신이 불가능합니다.</p>"

    @content.setter
    def content(self, content: str):
        self._config.html_content = f"{content}<br><br><p style='color:gray;'>* 본 메일은 발신 전용으로 회신이 불가능합니다.</p>"

    @property
    def sender(self) -> DataDict:
        return self._config.sender

    @property
    def subject(self):
        return self._config.subject

    @subject.setter
    def subject(self, subject: str):
        self._config.subject = subject

    @property
    def to(self) -> DataDict:
        return self._config.to

    @to.setter
    def to(self, to:Union[Dict[str, str], DataDict]):
        for name, email in to.items():
            self._config.to[name] = email

    def add_to(self, **receivers):
        for name, email in receivers.items():
            self._config.to[name] = email
        return

    def attach(self, file):
        with open(file, "rb") as f:
            file_content = base64.b64encode(f.read()).decode('utf-8')
        self._config.attachment = [{
            "content": file_content,
            "name": os.path.basename(file) # 수신자가 보게 될 파일 이름
        }]

    def del_to(self, *receivers):
        keys = []
        for receiver in receivers:
            if "@" in receiver:
                for name, email in self.to.items():
                    if receiver == email:
                        keys.append(name)
                continue
            if receiver in receivers:
                keys.append(receiver)
        for key in keys:
            del self._config.to[key]
        return

    def send(self):
        conf = self._config.copy()
        conf['to'] = []
        for name, email in self.to.items():
            conf['to'] = [{"email": email}]
            smtp = sib_api_v3_sdk.SendSmtpEmail(**conf)

            try:
                self._sender.send_transac_email(smtp)
            except (ApiException, Exception) as e:
                self.logger(f'FAILED TO SEND EMAIL FOR :{name} / {email} - {e}')
        return


if __name__ == '__main__':

    mail = Mailing(api='')
    mail.to.manager = "snob.labwons@gmail.com"
    mail.sender.name = 'TESTER'
    mail.subject = f'[TEST] TESTING'
    mail.content = "Hello World!"

    print(mail)
    # mail.send()