import smtplib

class notifications(object):

    def __init__(self):
        self.username = "genomic.analysis.ecker"
        self.password = "g3n0m3analysis"
        self.FROM     = "genomic.analysis.ecker@gmail.com"

        self.admin    = ["jfeeneysd@gmail.com","jnery@salk.edu","ronan.omalley@gmail.com"]
        # self.admin = ["jfeeneysd@gmail.com"]

    def send_message(self,TO,SUBJECT,TEXT):
        """
        ARGS:
        - TO      -> a list of recipients
        - SUBJECT ->
        - TEXT    ->
        """

        message = """\From: %s\nTo: %s\nSubject: %s\n\n%s""" % (self.FROM, ", ".join(TO), SUBJECT, TEXT)

        try:
            server = smtplib.SMTP("smtp.gmail.com",587)
            server.ehlo()
            server.starttls()
            server.login(self.username,self.password)
            server.sendmail(self.FROM,TO,message)
            server.close()
            print("Notification to %s successful" % " ".join(TO))

        except smtplib.SMTPRecipientsRefused:
            print("Couldn't send email to %s. Skipping Message. Bcl Script Continues" % (",".join(TO)))

        except Exception,err:
            sys.stderr.write('ERROR: %s -> Not Sending Message. Script Continues\n' % str(err))

    def admin_message(self,SUBJECT,TEXT):
        self.send_message(self.admin,SUBJECT,TEXT)

if __name__=="__main__":
    print("Test....")

    email = notification()

    email.send_message(["jfeeneysd@gmail.com"],"Test","This is a test")