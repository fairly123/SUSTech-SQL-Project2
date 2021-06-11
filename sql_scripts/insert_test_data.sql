begin;

insert into course (id, name, credit, class_hour, is_pf_grading)
values ('CS102A', '计算机程序设计基础A', 3, 32, false);
insert into course (id, name, credit, class_hour, is_pf_grading)
values ('CS202', '计算机组成原理', 3, 32, false);
insert into course (id, name, credit, class_hour, is_pf_grading)
values ('CS208', '算法分析与设计', 3, 32, false);
insert into course (id, name, credit, class_hour, is_pf_grading)
values ('CS307', '数据库原理', 3, 32, false);
insert into course_prerequisite_relations (course_id, prerequisite_id, and_logic)
values ('CS307', 'CS102A', null);
insert into course_prerequisite_relations (course_id, prerequisite_id, and_logic)
values ('CS307', 'CS202', null);
insert into course_prerequisite_relations (course_id, and_logic)
values ('CS307', true);
insert into course_prerequisite_relations (course_id, prerequisite_id, and_logic)
values ('CS307', 'CS208', null);
insert into course_prerequisite_relations (course_id, prerequisite_id, and_logic)
values ('CS307', null, false);

insert into department (name)
values ('CSE');
insert into major (name, department_id)
values ('ComputerScience', 1);
insert into student (id, first_name, last_name, enrolled_date, major_id)
values (11911413, 'Isaac', 'Moore', '2001-08-06', 1);
insert into semester (name, begin_date, end_date)
values ('2021Spring', '2021-3-1', '2021-8-31');
insert into course_section (course_id, semester_id, name, total_capacity, left_capacity)
values ('CS102A', 1, 'No.2 class of CS102A', 40, 40);
insert into course_section (course_id, semester_id, name, total_capacity, left_capacity)
values ('CS202', 1, 'No.1 class of CS202', 40, 40);
insert into course_section (course_id, semester_id, name, total_capacity, left_capacity)
values ('CS208', 1, 'No.2 class of CS208', 40, 40);
insert into course_section (course_id, semester_id, name, total_capacity, left_capacity)
values ('CS307', 1, 'No.2 class of CS307', 40, 40);
insert into student_section_relations (student_id, section_id)
values (11911413, 1);
insert into student_section_relations (student_id, section_id)
values (11911413, 2);
insert into student_section_relations (student_id, section_id)
values (11911413, 3);
commit;