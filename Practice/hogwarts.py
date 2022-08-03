students = ["Hermione","Harry","Ron"]

for student in students:
    print(student)

for i in range(len(students)):
    if i < 2:
        print(i + 1, students[i])
        i = i + 1