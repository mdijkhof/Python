def main():
    planningLeadtime()

def planningLeadtime():

    national_approval_notes = ['package 1','package 2','package 3','package 4','package 5']
    
    for notes in national_approval_notes:
        if 'package 1' in notes.lower():
            leadtime = 10
        elif 'package 2' in notes.lower():
            leadtime = 10
        elif 'package 3' in notes.lower():
            leadtime = 5
        else:
            leadtime = 2
        print(leadtime)
    return leadtime

main()