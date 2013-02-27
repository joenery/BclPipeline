import smtplib

class notification(object):

    def __init__(self):
        self.username = "genomic.analysis.ecker"
        self.password = "g3n0m3analysis"
        self.FROM     = "genomic.analysis.ecker@gmail.com"

        self.admin    = ["jfeeneysd@gmail.com","jnery@salk.edu"]
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

    def bcl_complete_blast(self,run,owners_and_samples,bcl_output_dir):
        """
        ARGS:
        owners_and_samples: A dicitionary where Keys are mapped to email address and a list of samples are the values. This is created in Sample_sheet parser
        """

        for email in owners_and_samples.keys():

            # Get a list of all the samples paths that was associated with that person.
            samples = []

            for tup in owners_and_samples[email]:
                project = tup[0]
                sample  = tup[1]

                samples.append(run + "/" + bcl_output_dir + "/Project_" + project + "/Sample_" + sample)

            text = " Your files are located at: \n\n%s\n" % "\n".join(samples)

            self.send_message([email],"Bcl Run Complete!",text)

    def admin_message(self,SUBJECT,TEXT):
        self.send_message(self.admin,SUBJECT,TEXT)

    def bcl_start_blast(self,run,owners_and_samples):
        """
        Mass email to all those who have samples in the run
        """

        if len(owners_and_samples) != 0:
            self.send_message(owners_and_samples.keys(),"Bcl Analysis Has Started for %s" % (os.path.basename(run)),"Just a friendly reminder from GAL-E to let you know that the following run: %s has started it's Bcl Analysis.\n\nI'll send you an email with the path(s) to your sample(s) on Oberon when it's done.\n" % (os.path.basename(run)))

        else:
            print("No one to BCL_Start_Blast")

if __name__=="__main__":
    print("Test....")

    email = notification()

    email.send_message(["jfeeneysd@gmail.com"],"Test","This is a test")