;��һ��:�۽����Ϊ����,title:���Ϊ,"text",controlId:дID����ʶ��
ControlFocus("���Ϊ","text","40965")
;��ͣ�ű���ִ��ֱ��ָ�����ڴ��ڣ����֣�Ϊֹ
WinWait("[CLASS:#32770]","",10)
;�ڶ���:����ļ�����ַ,����$CmdLine[1]����exeִ��ʱ�Ķ�̬����,
;���� kuang.exe "D:/test/a.html",�����Ϳ��Զ�̬�ı��ַ�����֣�ͨ��python
ControlSetText("���Ϊ","","Edit1",$CmdLine[1])
;��ʱ����
;Sleep(1)
;������:������水ť,��������,title:���Ϊ,"text"д�ɿ�,controlId:д��Button2��ClassnameNN��Ҳ����ʶ��
ControlClick("���Ϊ","","Button2")