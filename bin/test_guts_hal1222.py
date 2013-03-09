from guts import *

if __name__=="__main__":
    print("Testing...")

    p = project(run_path="/mnt/thumper-e4/illumina_runs/130213_HAL_1222_AC112TACXX/",sample_sheet="SampleSheet.csv",bcl_output_dir="ChlamyTest")

    print("Parsing Sample Sheet")
    p.parseSampleSheet()

    # print("Running ConfigureBclToFastq")
    p.runConfigureBclToFastq()