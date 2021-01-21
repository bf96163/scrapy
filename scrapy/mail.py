"""
Mail sending helpers

See documentation in docs/topics/email.rst
"""
import logging
from email import encoders as Encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.nonmultipart import MIMENonMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from io import BytesIO

from twisted.internet import defer, ssl

from scrapy.utils.misc import arg_to_iter
from scrapy.utils.python import to_bytes


logger = logging.getLogger(__name__)


def _to_bytes_or_none(text):
    if text is None:
        return None
    return to_bytes(text) #就是text.encode 不过加了些判断是否已经是bytes


class MailSender:
    def __init__(
        self, smtphost='localhost', mailfrom='scrapy@localhost', smtpuser=None,
        smtppass=None, smtpport=25, smtptls=False, smtpssl=False, debug=False
    ):
        self.smtphost = smtphost
        self.smtpport = smtpport
        self.smtpuser = _to_bytes_or_none(smtpuser)
        self.smtppass = _to_bytes_or_none(smtppass)
        self.smtptls = smtptls
        self.smtpssl = smtpssl
        self.mailfrom = mailfrom
        self.debug = debug

    @classmethod
    def from_settings(cls, settings):
        return cls(
            smtphost=settings['MAIL_HOST'],
            mailfrom=settings['MAIL_FROM'],
            smtpuser=settings['MAIL_USER'],
            smtppass=settings['MAIL_PASS'],
            smtpport=settings.getint('MAIL_PORT'),
            smtptls=settings.getbool('MAIL_TLS'),
            smtpssl=settings.getbool('MAIL_SSL'),
        )

    def send(self, to, subject, body, cc=None, attachs=(), mimetype='text/plain', charset=None, _callback=None):
        from twisted.internet import reactor
        if attachs:
            msg = MIMEMultipart()
        else:
            msg = MIMENonMultipart(*mimetype.split('/', 1))

        to = list(arg_to_iter(to)) #arg_to_iter 就是如果是None就返回None 如果是__iter__就返回本身 其他返回[传入参数]
        cc = list(arg_to_iter(cc))

        msg['From'] = self.mailfrom
        msg['To'] = COMMASPACE.join(to)
        msg['Date'] = formatdate(localtime=True) #默认就是当前时间戳
        msg['Subject'] = subject
        rcpts = to[:] #deepcopy
        if cc:
            rcpts.extend(cc)
            msg['Cc'] = COMMASPACE.join(cc)

        if charset:
            msg.set_charset(charset)

        if attachs:
            msg.attach(MIMEText(body, 'plain', charset or 'us-ascii'))
            for attach_name, mimetype, f in attachs:
                part = MIMEBase(*mimetype.split('/'))
                part.set_payload(f.read())
                Encoders.encode_base64(part)
                part.add_header('Content-Disposition', 'attachment', filename=attach_name)
                msg.attach(part)
        else:
            msg.set_payload(body)

        if _callback:
            _callback(to=to, subject=subject, body=body, cc=cc, attach=attachs, msg=msg)

        if self.debug:
            logger.debug('Debug mail sent OK: To=%(mailto)s Cc=%(mailcc)s '
                         'Subject="%(mailsubject)s" Attachs=%(mailattachs)d',
                         {'mailto': to, 'mailcc': cc, 'mailsubject': subject,
                          'mailattachs': len(attachs)})
            return #debug以后 就不进行真的邮件发送了
        ## 这里的dfd是我们self._sendmail返回的deferred对象
        dfd = self._sendmail(rcpts, msg.as_string().encode(charset or 'utf-8'))
        # deferred常用的回调添加方法
        dfd.addCallbacks(
            callback=self._sent_ok,
            errback=self._sent_failed,
            callbackArgs=[to, cc, subject, len(attachs)],
            errbackArgs=[to, cc, subject, len(attachs)],
        )
        #添加trigger 让他这个在结束前发出
        reactor.addSystemEventTrigger('before', 'shutdown', lambda: dfd)
        return dfd

    def _sent_ok(self, result, to, cc, subject, nattachs):
        logger.info('Mail sent OK: To=%(mailto)s Cc=%(mailcc)s '
                    'Subject="%(mailsubject)s" Attachs=%(mailattachs)d',
                    {'mailto': to, 'mailcc': cc, 'mailsubject': subject,
                     'mailattachs': nattachs})

    def _sent_failed(self, failure, to, cc, subject, nattachs):
        errstr = str(failure.value)
        logger.error('Unable to send mail: To=%(mailto)s Cc=%(mailcc)s '
                     'Subject="%(mailsubject)s" Attachs=%(mailattachs)d'
                     '- %(mailerr)s',
                     {'mailto': to, 'mailcc': cc, 'mailsubject': subject,
                      'mailattachs': nattachs, 'mailerr': errstr})

    def _sendmail(self, to_addrs, msg):
        # Import twisted.mail here because it is not available in python3
        from twisted.internet import reactor
        from twisted.mail.smtp import ESMTPSenderFactory
        msg = BytesIO(msg)
        d = defer.Deferred()
        factory = ESMTPSenderFactory(
            self.smtpuser, self.smtppass, self.mailfrom, to_addrs, msg, d,
            heloFallback=True, requireAuthentication=False, requireTransportSecurity=self.smtptls,
        )
        factory.noisy = False

        if self.smtpssl:
            reactor.connectSSL(self.smtphost, self.smtpport, factory, ssl.ClientContextFactory())
        else:
            reactor.connectTCP(self.smtphost, self.smtpport, factory)

        return d
