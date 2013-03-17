@echo upload file to pubftp
:cp file to other os
echo open 110.75.5.128>ftp.txt
echo pubftp>>ftp.txt
echo look>>ftp.txt
echo binary>>ftp.txt
echo put target/dueam-jobs-jinyuan.jar>>ftp.txt
echo bye>>ftp.txt
ftp.exe -i -s:ftp.txt
@echo upload finis
@pause