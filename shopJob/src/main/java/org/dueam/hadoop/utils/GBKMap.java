/*
 * Created on 2004-11-14
 */
package org.dueam.hadoop.utils;

import java.io.*;


/**
 * @author dogun
 */
public class GBKMap {
    private static final String GBK_T2S_T_MAP = "�G�K�y�������Ёсׁ����H�I�R�S�b�t�z�}�������������Ȃɂʂ̂΂܂���������������A�E�H�L�S�W�^�e�f�l�r�x�z�|�}�~�������������������������������������ȃɃԃ����C�E�I�P�R�e�h�q�t�w�}�������������������������������ńӄԄՄׄلڄ݄ބ��������Q�R�T�U�V�X�^�f�r�s���������������������ǅΆJ�T�U�h�r�w���������������������ǆʆˆ̆Άц܆݆߆����@�D�I�K�L�O�W�Z�[�\�^�`�c�f�j�u�z�}�����������������������������������������Ƈʇˇ̇χ҇ӇՇڇ߇���������@�A�D�F�s�������ƈ̈Ԉ׈߈������A�K�L�N�P�T�V�]�_�j�m�q�t�|�������������������������������ĉŉƉȉΉωщ؉ډۉމ��A�J�W�Y�Z�^�y�������ʊՊ�D�H�I�z�����������������������Ƌȋɋԋ؋܋ߋ���������D�O�W�\�m���������������������������������ÌƌȌόҌӌՌٌڌ��s�u�{�������������������������������A�F�G�M�S�V�X�Z�[�h�n�p�r���������������������ÎŎ͎Ύӎ֎׎�������B�F�J�N�P�R�S�T�U�V�[�]�d�h�i�t���������������������������������ďƏ͏ϏՏؐa�u���������������Őېܐ�������@�B�C�K�L�M�Q�T�U�Y�Z�]�a�b�c�h�j�n�v�z�{�|�������������������������������Ñ͑Бёґԑ֑בّؑߑ��������L�����ΒВԒߒ��������P�Q�]�d�k�l�p�u�v���������������������Ɠ͓Γϓӓדۓܓ�������������������D�E�F�H�M�P�Q�R�S�U�X�[�\�]�_�d�f�n�r�t�v�x�y�z���������������������������������̔ؔ���N�r�x���������������ϕѕҕԕڕ���������V�|���ŖʗU�d�g�l�n�y����������������������E�I�O�o�q�s�������������������������Řǘ˘ИӘ������C�E�M�R�_�f�n�u�x�z�{���������������������������������������əΙЙљڙ��������J�U�W�^�a�e�g�q�v�w�{���������������������������КӚؚ֚ښ��������@�A�Q�Z�]�_�r�����ћܛ���D�I�O�Q�R�S�Y�Z�\�o�p�t�u�y���������������ʜϜ؜ۜݜ����������B�F�G�I�L�M�O�U�a�h�i�n�q�s�u�{�}���������������������������������Ɲɝ˝͝ҝ՝��������������E�F�H�I�N�R�T�V�]�^�a�c�g�l�o�r�t�u�z�{�|���������������������������Ğݞ���N�o���������������������ɟ͟�������������C�F�I�N�S�T�Z�`�a�c�d�q�t�{�������������������������ΠӠ٠ޠ�M�N�b�q�s�w�y�z�{�������������@�C�E�F�H�I�J�M�R�k���F�P�g�m�q�z�|���������������I�\�^�a�c�h�m�t�v���������T�U�Y�Z�a�b�d���������������B�d�i�w�{�����������������������A�B�D�K�O�T�V�W�X�Y�[�]�_�`�a�b�c�d�l�}���������I�K�M�O�P�R�U�i�x�{�������A�g�m���������C�h�p�����������T�X�^�_�a�o�u�~���������A�K�V�Z�[�\�^�a�v�z�������B�R�U�Y�[�\�d�i�������A�M�N�Q�Y�d�e�f�v�w�x�~�������C�D�F�G�H�M�Q�Z�[�]�^�`�p�w���P�S�a�w�{�~�������B�D�O�S�V�Y�`�j�t�w�����������������@�I�L�U�X�[�\�^�`�a�e�f�h�j�n�������R�S�U�Z�a�c�e�g�m�o�q�s�t�u�v�w�x�y�{�~���������������������������������������B�C�E�H�I�K�L�M�O�V�W�X�Y�^�d�f�g�j�k�o�q�x�y�z�{���������������������C�E�G�I�J�Q�R�S�T�U�V�W�X�Y�Z�]�^�_�`�b�c�d�i�l�o�p�r�w�x�y�|�}�~�������������������������������������@�A�G�M�N�O�P�U�V�Z�\�_�`�b�c�d�h�l�p�r�s�t�v�w�y�z�{�|�~�������������������������@�C�D�H�K�L�M�O�P�Q�R�U�[�^�_�`�h�i�k�m�n�p�t�w�y�|�������������P�R�T�U�_�`�b�d�s�u�w�x�����E�N�O�P�Z�a�e�g�} �@�C�{�|�}ÄËÑÓÛ�I�L�T�X�[�_�c�d�e�s�w�zāđĒēĘĚĜğ�D�F�K�L�N�P�R�V�_�c�d�e�f�m�oœŚśŜŞ�@�A�D�G�H�c�rƝ�G�f�o�v�{�A�C�O�R�f�n�~ȇȐȒȔșȝ�L�O�P�W�n�p�t�wɆɉɏɐɔɜ�C�N�V�Y�[�\�a�h�n�r�w�{�|�~ʁʉʎʏʒʚ�C�E�G�K�N�R�W�]�_�j�{�|ˇˎ˒˞�@�A�E�I�J�K�N�O�\�`�d�m�y�}̖̝̎̓̔�A͐͑͘�g�o�r�t΁·ΕΛΞ�N�Q�U�X�\�l�s�u�x�|ρωϊϐϓϔϖϞϠ�D�M�Q�U�\�`�g�k�l�n�o�}Ж�U�W�Y�a�b�e�u�}тыѝў�@�C�L�M�S�U�\�c�d�h�m�r�u�w҇ҊҍҎҒғҕҗқҠ�C�D�H�J�M�N�P�U�X�Y�[�]�^�h�x�z�|ӅӆӇӋӍӏӑӒӓӕӖӘәӚӛӞӠ�A�D�E�G�H�K�L�O�S�V�X�\�]�^�b�g�n�p�r�t�u�v�w�x�{�~ԁԂԃԄԇԊԌԍԎԏԑԒԓԔԖԜԞԟ�C�D�E�F�I�J�N�O�Q�T�V�Z�\�]�_�`�a�b�d�f�h�l�n�r�u�x�{�~ՁՄՆՈՊՌՎՏՓՔ՘ՙ՚՛՞՟�@�B�C�G�I�J�M�O�R�S�T�V�X�Z�\�]�^�`�a�e�i�k�o�q�r�t�u�v�x�{փֆև֎֔֙֜�@�C�F�H�I�P�R�S�T�V�Y�d�e�g�h�l�o�p�u�v�x�y�}׃ׇׅׄ׉׋׌׎׏דהוח�G�M�Q�S�W�i�r؂؈ؚؐؑؒؓؔؕ؛؜؝؞؟�A�B�D�E�F�H�I�J�L�M�N�O�Q�R�S�T�U�V�W�Y�Z�[�\�_�c�d�e�f�g�k�l�m�n�p�r�s�t�u�v�x�y�|�}�~ـفهوًٍَُِّٗ٘ٚٛٝٞ٠�A�B�E�F�H�I�M�N�X�s�wڅڎ�E�R�`�m�xۄۋ۔ەۘۙ�E�J�O�P�Q�S�U�V�W�X�]�b�f�g�k�n�|܀܇܈܉܊܍܎ܐܗܛܠ�F�M�S�T�U�V�W�Y�^�`�b�c�d�e�m�n�o�p�v�w�x�y�z݂݆݈݁݅݉݋ݏݔݗݘݚݛݜݞݠ�@�A�D�H�I�O�Z�\�]�_�i�k�o�p�q�r�s�~ޒޕޟ�@�B�L�M�[�\�^�_�`�b�d�e�f�h�m�t�w�x�z�|�~߀߃߅߉ߊ�A�P�S�]�i�l�u�w�x�y�{���������������B�O�Z�d�h�j�t�u�w�~�����������������@�A�C�F�O�Q�S�T�Z�[�]�^�_�b�c�g�h�j�k�l�n�o�x�{�}������������������@�B�C�E�G�K�O�Q�T�U�X�\�^�`�f�g�l�o�q�s�t�x�y�|�~��������������@�A�B�C�D�H�I�J�N�P�R�S�X�Y�Z�\�^�b�e�h�o�r�s�u�y�z�{�|�~����������������E�F�H�K�N�O�P�Q�U�V�W�X�\�^�_�a�d�e�i�l�n�r�u�v�x�{�|�}�������������@�D�I�J�N�P�R�V�X�[�^�_�g�i�k�m�n�t�u�v�y�z�|�}��������������B�C�H�I�K�M�N�O�P�R�S�U�Y�a�f�h�j�n�t�|��������������C�D�F�G�I�K�O�T�Z�\�a�b�d�e�k�n�p�s�t�u�z�|������������L�T�V�W�Z�\�]�_�`�b�c�e�f�g�h�l�m�u�v�w�x�y�|�}������������������@�A�D�F�G�H�I�J�N�P�R�T�U�V�X�Y�l�n�������������A�E�F�H�O�S�U�[�]�_�`�b�h�m�p�r�s�u�x�y�����C�F�V�Z�\�^�`�a�n�o�r�t�v�z����F�X�\�^�a�d�f�g�h�n�t�w�x�y���������������@�A�B�C�D�H�I�M�R�U�W�\�^�_�a�c�e�h�i�j�l�n�w�}�~���������������@�A�B�D�E�L�Q�R�S�U�W�Z�\�^�_�`�d�g�h�j�k�l�w�|�}����������������A�B�D�E�F�G�H�I�K�L�N�P�Q�R�T�W�^�f�h�j�k�l�o�q�r�s�t�v�x�z�}�~�����������������R�S�T�W�Y�Z�_�g�v�w�x�z�{�~������������E�G�H�K�R�S�T�U�V�\�]�_�i�j�q�s�t�v�|�}�~���������������@�A�E�H�J�K�L�P�Q�i�t�v�w�x�y�����E�P�W�Y�Z�[�\�]�a�b�d�u�|�~������������E�G�N�O�P�Q�R�T�U�V�W�^�`�b�j�n�o�q�r�w�z�{�~�����������������������A�E�F�H�K�L�N�O�T�X�Y�[�a�c�e�f�g�l�m�p�q�s�t�v�w�������������������������������������@�B�F�I�L�M�V�W�X�Z�[�\�a�c�d�g�h�k�l�q�s�{�|�~�B�D�F�O�P�Q�S�c�d�f�o�r�x�z�{�|���������������������@�M�N�O�P�Y�Z�[�]�^�g�i�k�l�o�s�t�{�����������������A�B�F�I�L�O�Q�V�W�X�Y�\�]�^�_�a�g�p�s�t�v�w�������������������������D�I�K�L�R�U�W�X�Z�[�u�y�z�|�}�����������D�I�N�S�Z�c�h�o�q�s�t�w�x�{���������B�O�R�S�V�W�X�Z�[�]�_�b�e�f�g�i�l�m�p�r�x�|�}�������������݃J�ك�x�����ȅՉ����Ќ����m�ُb�s���A�K�M�W�\�^�d�w���{�ڕ��ŖX�g�������ɠ������X�q�r���G�[�t�n�o�H�u���x�uÏ�Sƈ�^˟�Y�o�b���s�j�z�N�i��a�N�@";
    private static final String GBK_T2S_S_MAP = "������ب�Ƿ�����ռ�������¾�ٶϵ�������ָ����ҷ���ΰ����͵��α����ɡ��Ч��Ӷ�̴���ծ�����ͽ�������α���ǹͼ���ٯ�ڵ������ٱ٭�����Ŵ�ٳ��������ٲ�׶Ҷ�����������Ϳ�����´���ƾ��ɾ����˄i�հ����ܴ��������������۽�������������ѫʤ���Ƽ��˄�۽ѫ��Ȱ���л�����������Э��ȴ���������᳧���ɲδ�������Ա�������������������|���ۻ���ɥ���ǵ�ӴǺ�Ćy��������̾�Ż�����黩��Хߴ��߼����������������������ֵ������Ⳣ������߿������������������߽�������شѻع����Χ԰Բͼ�����������ִ����������Ң�������������Ϳڣ��������ǵש��׹�Ͷ�̳�؈�ǽ��̳����ѹ������̳��¢¢�ްӉG׳�������ٹ��μ�ۼ���ƶ��ױ橼�ֶ����¦������͵������������测O��濽�������������Ӥ���������ѧ�Ϲ���ʵ����д���豦�˽�רѰ�Ե����Ͻ�ʬ�����Ų��������᭵�Ͽ��������������������ոᫍ���ὍiọN�����������������������˧ʦ�ʴ�֡�������ıҰ����[�ɼ��������Î�����������зϹ���®�����˵�����ǿ��ǿ��������͏����𾶴��⸴��������������������������������������̬㳲ҲѲ�����������������������Ǳ���ƾ㴑\�������������Ӧ������������������������延���������ϷսϷ��Ǥ��Ю���Ѿ�ɨ�Ւ����Ҳɼ��ﻻ�ӱ�������ҡ����ե����§��ֿ���Ҳ��̒��������ص���������̢�μ�ӵ°�������Я�ݼ�̧�������š������ߢ����ߣ������£������ߥЯ������̯�������������������������ն������ʱ�������͕D����������������ӿ����oɹ����ʶ���դ�ո����������������涰ջ���ܗ��������ҵ����������̹�ǹ����餽���׮������¥�����������������Ż��ֺ������ߵ��혖���ɗ�̨����������������������ݳ���������������ӣ��Ȩ����������̾ŷХ��죻���������������������ɱ�ǻ�Źҽ�������ձձ��������ʷ������۾���û���й�����ݰ�������˾�����Ԩ�ǳ�����h�в������ӿ����׼������ʪ��������㻦����±䰛������朾Ž���������ӽ�����ý���Ǳ����������ɬɬ�ν��Խ��������䫵���Ũ�mʪŢ�ɛ������Ŀ�Ϋ��������������к��䯱�����������������������������̲�������������Ϊ�������ѻ����ů��������ӫ�����G�����������������Ӫ�ӻ����Ѭ����ҫ˸¯������Ϊү����ǽ��բ빵�ǣ������״���������������ʨ���������A����������̡���������������ܷ�������`����Ө���ūo���Q��訫����_����������ש��������Ķ�ϻ���������������������������ű�}���������������ݱ������֢���Ѣ��Ӹ̱񲷢�������屭��յ������¬�����������������������ɱ���������n��������˶����ȷ��}ש�������ͳ~��������������������»�����t��������ͺ��˰���������ֳƹ��ջ�ӱ�������Ȼ���������ҤҤ������������Բ����������ȸ�����颽ڷ�����������ɸ����¨����������ܹYǩ�������ٹ���������ǩԿ����������ױ�����ַ�ģ���������о�����Լ��������������Ŧ磴����ɴ��ֽ�������������ϸ����������������������祽����竽���Ѥ������ͳ˿签�������������������̳�����ά��纸�����׺��������������绽���������罼�����༩�е���Ե����໺��γ������������������������������и����������з����������������������ܼ�����������֯��ɡ����������������ϵ�������ؽ���������K�������۲�ӧ�����²�̳���̳�䷣��շ�����������������ϰ���̰����˳�����ʥ��������������ְ����������вв���ִ����������������׽ų������N�����嵨��ŧ����������������H���ٸ�̨���˾پ��̹ݲ�����������������ܳۻ���Ⱦ�ׯ�����Ȼ���������ݫҶݦ��ݧέҩ����ݻݪݰ��ݥϯ�ǲ�ݯ����ݻ���ⲷ�佯��������ݡ����ݤܿݵ���޵�����������ܼ��Ǿ�Qݲ��������ݣ��ҩ޴�°����[ޭ«����ƻ޺���������ܴ���²�ſ�������ʴ�Ϻʭ��������ө��΅���������ͳ�����Ӭ�Ы����������]�Ʋ�����������������ֻ�������ﲹװ���Ƹ��TЄ��������������B�����������Ϯ�[�˼��_���������������`���������������������۵�������ڥ������Ѷڧ����ڦךѵڨ���мǶ���������ګ��כ��������ڭ��ע֤ڬڮڪթڱگ��םלڰ���ӽڼѯ����ʫ��ڸ��ڹڵ������ڷڶכڴڳ��ڲ��־��ڿ������ڽ��Ͻ�����ھ�л�˵˵˭���Ƿ������׻̸����ں�����������ĵ�נ����ڻ����г�����ѻ����ȷ�������ŵı��ν���߻��������հ�ǫ�ֽ�лҥ������ک��á����֤���ܼ���ʶ��̷�����޻�����Ǵ��מ���ٶ������Ԁ�����Ų���������߽����Ϫ�����������k��è�����O���ƹ�ƶ����̰���������߷��������ܷ�����ó����¸�޻����ʼ����������ޱ������Q���޴����������������������˶������Rʣ׬�繺������׸�S��������Ӯ�����P���͸����W�����������ּ���ӻ���ϼ����������Q������Ծ�������������������������������a���������������������������������b�������츨����ꢻ���꡹��������c���������dշ���d�Ͻԯ�ת�޽�ꥺ�������ǰ�Ǳ��ũũ�ƻ��˾������ܽ����˹���Υңѷ����Զ�ʳ�Ǩѡ���������Ǳ����κ�ۣۧ��۩����������ܭ��֣�ڵ���ۦ��۪�����ͳ���ҽ���N�����������������������ȶ�������̿��˷���������ǥ�������Կ���ƶ۹�����蕳�ť���Ӹ�������������������������������������������鲬��ǯíǦ�Ყ���������虽���������ͭ�ϳ���������������ҿ����������������������������п���������������������������ﳾ�����¼������׶�￴������Ķ��Ǯ��ê��������̱��������������A��������ա薶��B����������̼���������þ���Ѱ�Ͻ�F���Ӵ��D�C����������˸������E�������������������H������������޲����������G�����������������������������������������I�����������ټ������@���J��������¯��Կ�K�����������������E�����������\�տ����������м���բ�ֺҹظ�Ϸ������������������������������ְ����������^���`�]���ڴ��������_�����a�˿�ַ������������½���������ӽ��������������¤����ֻ����˫���Ӽ������Ƶ�մ���������������酥������������ػػ����������ǧ��Τ��킺������������ҳ������˳������������Ԥ������������򤸩ͷ�H���F�I򥾱��Ƶ�ǿ��������J���Ը������˲�����­�ȧ���s��̨����t���u��vƮƮ��쭷ɼ����������������Ǳ������������������ٶ���������🽤�ڹ����ι����A��������@�������������ͼ��������ɲ�����Ԧ���Գ�ѱ�R��פ����������ʻ�������麧���S���V�����U���W����������ƭƭ���Y���������ɧ�������������������X���������⾪����¿�����Z���T�������������޷����ɺ����޶����ֺ��Ҷ�����������������³���������������������ر����������������������������������������������������������������������������������������������������������������������������������������������������������������������@����������������������\�������ѻ�_��ԧ��^����Ѽ�`���a�����b�c������ö�����������e��ȵѻ�g�d����f���j�����i�d�����]ݺ�l���m�n���������o�μ��k���pŸ�������h������������ӥ�����r��ݺ�s��t������±����������������������ô����㵳��ù�d����������������������ի��촳��������������������������ȣ�������ӹ���꼳��¾�о�ɲ����ų���������������Ӹ߱�����Ǥ���ղ���ҷ��ߦ������������������������������غ������ֻ����I�������й��������ڽ�����ǹϳ����°��أ";
    private static final String GBK_S2T_S_MAP = "�����������°Ӱհڰܰ����������������������������������ʱϱбұձ߱���������������������������������ƲβϲвѲҲӲԲղֲײ޲����������������������������������������������³ĳųƳͳϳҳճٳ۳ܳݳ�������������������������������������´ǴʴʹϴдѴӴԴմڴܴ�������������������������������������������Ƶ˵еӵݵ޵ߵ���������������������������������������ĶƶͶ϶жҶӶԶֶٶ۶������������������������������������÷ķɷ̷Ϸѷ׷طܷ߷�������������������������������øƸǸɸϸѸӸԸոָٸڸ����������������������������ƹȹ˹йҹع۹ݹ߹�����������������������������źҺ׺غ���������������������������������������������ƻѻӻԻٻ߻�����������������������������������������������üƼǼʼ̼ͼмԼռּؼۼݼ߼��������������������������������������������������������������������½ýĽŽȽɽʽνϽ׽ڽܽ������������������������������������������������ǾɾԾپݾ�����������������������ſǿοѿҿٿ���������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������¢£¤¥¦§¨«¬­®¯°±²³¸»¼½¿����������������������������������������������������������������������������������������áèêíóôùûþ������������������������������������ıĶ��������������������������������������������šŢťŦŧŨũűŵŷŸŹŻŽ������������ƭƮƵƶƻƾ������������������������������������ǣǤǥǦǨǩǫǮǯǱǳǴǵǹǺǽǾǿ����������������������������������������������ȣȧȨȰȴȵȷ��������������������������������������ɡɥɧɨɬɱɴɸɹɾ��������������������������������������ʤʥʦʨʪʫʬʱʴʵʶʻ����������������������������������˧˫˭˰˳˵˶˸˿����������������������������������������̷̸̡̢̬̯̰̱̲̳̾����������������������������������ͭͳͷͺͼͿ������������������������������ΤΥΧΪΫάέΰαγν����������������������������������������������������ϮϰϳϷϸϺϽϿ������������������������������������������������������������ХЫЭЮЯвгдклп����������������������������ѡѢѤѧѫѯѰѱѵѶѷѹѻѼ������������������������������������������������ҡҢңҤҥҩүҳҵҶҽҿ��������������������������������������ӣӤӥӦӧӨөӪӫӬӮӱӴӵӶӸӻӽӿ����������������������������������������ԤԦԧԨԯ԰ԱԲԵԶԸԼԾԿ����������������������������������������������������������������աբդթիծձյնշոջս������������������������������������������ְִֽֿ֣֤֡֯����������������������������������������������פרשת׬׮ׯװױ׳״׶׸׹׺׻��������������������������بػ����������������������������������٭ٯٱٲٳٶ��������������������������������ڣڥڦڧڨکڪګڬڭڮگڰڱڲڳڴڵڶڷڸڹںڻڼڽھڿ����������������������������������������������������������������������������ۣۦۧ۩۪ۻۼ۽������������������������������������ܫܼܳ����������������������������������ݡݣݤݥݦݧݪݫݯݰݲݵݺݻ��������������������ޭ޴޺޻����������������������ߢߣߥߴ߼߽߿����������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������i�օ����y�|���������G�׋O���N�i�������\�����D�o�������h�m�������霾���A�_�`�o���Q�}���n�}�~�t���Y�����������������������������N�H�Q�[΅�]Є�B�T�[�_�`Ԁךכלםמנ�k�O�P�Q�R�S�W�Q�a�b�c�d�N�������������@�A�B�C�D�E�F�G�H�I�J�K�\�]�^�_�`�a��F�G�H�I�J�K�r�s�t�u�v�����������@�A�R�S�T�U�V�W�X�Y�Z�������������������������������������������������������������@�\�]�^�_�`�a�b�c�d�e�f�g�h�i�j�k�l�m�n�o�p�r�s�t�d�������I���˹���������ɲ��̨����֢׼غܿ����ߦ߱������";
    private static final String GBK_S2T_T_MAP = "�}�@�K���\�W���T�[���C�k�O�ͽ��^�r������U݅ؐ�^�N��v���P�������]߅���H׃�q�p���M�e�T�l�I�e�PK�����K�g�N�aؔ���Q���M�K�N�nœ�}����ȃԜy��Ԍ�v���s�׋�p�P�a�U��L�L���c�S���n܇�؉m��r�ηQ���\�G�V�t�Y�u�X��_�x�����P�I�I�h���N�z�r�A���|̎�����J���N���b�o�~�n�[��ą����f�Z�e�_���J��������đ���Q�������hʎ�n�v�u�\���I���������f����c�|늝���{ՙ�B���Vӆ�G�|�ӗ����Y�٪��xـ�呔྄��ꠌ����D�g�Z���Z�~Ӟ���I�����D�E�l�P�y�m�\�C����؜��L���w�u�U�M�����^���S�S���h�L���T�p�S�P�wݗ���o�x�}ؓӇ�D�`ԓ�}�w���s���M����䓾V��怔R���w�t���o���m�ؕ�^�Ϙ�ُ���M�Y����P�^�^�T؞�VҎ���w���|܉Ԏ���F��݁�L假��^��n�h̖�u�Q�R�M�Z���t����o����W�A����Ԓ�щĚg�h߀���Q�������o�S�e�]�x���V�x���Z�R�M�d�Lȝ��ⷫ@؛�����C�e���E�I�u�����O݋���D���E����Ӌӛ�H�^�o�A�v�a�Z⛃r�{���O�Թ{�g�D�}�O�z�A�|���캆���p�]���b�`�vҊ�IŞ���T�u�R���K���{�Y�����v�u�z���ɔ��q�C�e�_��U�g�I�^�A���ܝ��Y�]�þo�\�H֔�M�x�a�M���G�o�L�@���i�o�R���d���Q�m���f�x�e��䏑ք��N���X�Q�E�^�x܊�E�_�P�w���n��������ѝ�F�K�~���V��r̝�h�Q�����U�Ϟ�D�R��ه�{�ڔr�@�@�m��׎���[���|���E�Ƅڝ����D��I�h�x�Y���Y������[�ўr�`�zɏ�B砑z�i����Ę朑ٟ����Z�����vՏ���|炫C�R���[�C�U�g⏜R�`�X�I�s�����@���\�Ŕn�]�Ǌ䓧�t�J�R�B�]�t���u̔���T�����H���X�H�ҿ|�]�V�G�n���\���y��݆�����S�]Փ�}�_߉茻j��j�����aΛ�R�R���I���u�~�}�m�z�U�M֙؈�^�T�Q�N�q�]�V�T�����i���i��Ғ��d���R�瑑�}�Q�և�\���c�{�y���X���[�H�ȔMā�f����B�m�懙������Q���o�~ē���r���Z�W�t���I�a�P�����r���i�_�h�lؚ�O�{�u���H��䁃W���V���DĚ�R�T�M���◉ә���L�T�U�w���t�X�Q���\�l�q���ܠ��N�����@��̃S�N�[�`�J�H���p��A�Ո�c���Fڅ�^�|��x�E����s�o�_׌���_�@���g�J�x�s�qܛ�J�c�����_�wِ������}�ߝ������Y��h�W�٠�����p���B�d�z���O�������I�B�K���}���{��Ԋ�ƕr�g���R���m��ҕԇ�۫F��ݔ���H���g���Q�����p�l����f�T�q�z��Z��A�b�\�K�V�C�m�S���q�O�p�S�s���i�H��B��؝�c�����TՄ�U���C���dӑ�v�`�R�}�w�ϗl�N�F�d �N�~�y�^�d�D�T�F�j͑Ó�r�W�E�D�m�����B�f�W�f�`����H�SȔ���ξ��^�l���y�����Y��΁�u�C�P���u���@�_�oʏ�ǉ]�F���`�a���u��㊑��rݠ�{�b�M�B���v�r�w�y�t��e�@�U�F�I�h�W�w��������lԔ��ʒ���N�ԇ[ϐ�f���y�{�C���a�x�\��d���n�C̓�u��S���w�m܎���x�_�k�W��ԃ���ZӖӍ�d���f������Ӡ鎟��}�����W�������V�����P��ꖰW�B�Ӭ��u���b�G�{ˎ��퓘I�~�t��U�z�xρˇ�|���xԄ�h�x�g���[�a��y��[�ы������t��Ξ�I��ω�A�f�ѓ��b�xԁ�����n�]♪q�[�T�ݛ�~�O���c�Z�Z�d�n�R�z�u�A�S�x�Y�@�@�T�A���hs�S耎[�������y���E�\�N�j����s���d����ٝ�E�v菗�؟��t���\ٛ����܈��l���p�S���ֱK��ݚ�䗣��`���q���~Û�w�U�H�N�@ؑᘂ��\�ꇒ걠�b�������C���̼����S���|��犽K�N�[���a�S�����E�i�T�D�T�����A�T�B�v���u�Dٍ���f�b�y�Ѡ��F٘���YՁ����Ɲ�Y�nۙ�C���v�u�{�M荁���d�݅����P�v�I�Q�Tّ�q�������t��Ѓ��z�������R�f��E�������L�e�Z���D��Ж�C�L�A�VӅӓӏӘ֎�n�G�b�X�g�t�x�r�E�CԟԑԜԖԍԏՊ՟Ԃ�V�a�N�OՌՎՆ՘Ք�~�r�R�G�o�]�@�I�X�O�B�J՛փו�q՞�kֆ�v�P�S�Hח�d׏�������w���P���S�i�B�c�J���͚뎀�׉�����ŉ����N�s�߉P��_���|ԭ�H�G˞�{�Oɐ�r�d���\�L��ʁɜ�w�C�j�Μ��n�|�{�pȇȒ�P�nɉ�W�W�~�Lɔ�M�rʉ�V��y�vʚ�`�A�I˒�\�Y���Г��ד�������d�t�X�]�x�\�`�҇��h�J�w�U���z�j�}�^���ɇ���߸���O�Z��r�K�ʺ�D���˺Ǉ��\�����o�����Ύ����L��s�S�����F���������V���⼹�p��Ȯ�E�w�����s���M�J�o�h��q�����A�G�N�Q�l�t�x�}�~�����T�s�[�ԑ��Y�����푫�Q�ÐŐ���a�ܑC�|���V�Z��b�h�`�Y���b����]������H�D�I�R�ݞ����{�o�T�ܛќ��Ҟg�G���Z���^�ƞc�O�s�����]���u�t�E���z�|���q߃ޟߊ�Տ����������I�ƌD�z���ȋ��܋�ԋ��z���|�A�w�~����P�U�S�K��s�\��t�~����K�J��u�q�v�w�k�������C�X���U�E�I�H�f�W�{�������c�_�p�y�i�E�R�^�J�U�l�~�|�����D�������P���N�b�d�c�r�O�V�_�~�z�w�����i�����\�`�R�Q�yô�^�|�k���z휭t�q�I�a���v���t�y�w�q���g�����n���ə��d���f����E����u�藨�������嘡����Ι����{���������_�{����������ܐܗ�M�V�_�T�W�Fܠ�]�U�Y�e�b�`�m݂�y�z�wݏ�A�O����T�ҕϕ����S�B�L�Oٗ�D�W�B�c�l�g�V�yَҗ�JҠ�]�D�M�P�U���ӚКښ�蠩�d�V�L�FÄĒ�T�Z�t�e�|Ĝ�e�R�S�Z�`�j�jݞ�W�̟������͟�F��������c�[���U���✡�����e�ʹ��X���^�Z�a�������~���L���g�{�A�����`�b������Q�A��{�O�S�b��k�j�[��^��Z��`���O���X�f�g��p��C�B�G���I�D��s�B��e�t�y�K�H��z��b�A�f��x�|��P�C�|�|�@���n��H�{���~�S�s��l��Z�u�|�H���Q�u��K�d�x���U�O��|�J���}�D�X�U�I��Y��k��r�y���\�S�M�N��O���C���h�u��|���j��Z�C�O�d�s�R���w�F�S�d�c�����R�z�|�����v�����[�P�Z�]�O���Y�^���g�l�������\�B�F�_�Y�W�p�w���������I���X�X�O������{�A�B�D�����`�a�]�dğ�]�M�dў�c�M�@�h�����g�e����@�R�M�}�W�h���D�����A�A�lϊ͘ϖϠ�|͐�u·ϓ�Xϔ�N�\���V�a�e�`�~�j�D�X�������f�[Ŝ�A���u�i�c�g�R�f�{��ڎ�����z�Oۄە�V�Eۋ�]�Q�W�U�bۘ�X�k�g�x�z�n�Z�V�\�Z�e�_�f�b�l�r�p�}�w�x���hׇ��Y�����E�G�T�|�d�V�c�T�q�^�n�b�q�o���\���~���������������a���N�O�E�H�K�A�F�T���X���a�l�{�q�v�m�c���������������B�L�����I�Z�X�V�k�^�X�d�x�X�t�y�x�|�u����W�t�o�B�O���}���a�^�߆��Ӈc�����ωڋ��ڎG�A�M��F������͕�����x�t��۝I���ݝU�˪��m�����c�\�������o���B�v���U���������x���x�T�Z���\�P�E�s�N�R�E�Q�Dы�Uт�wҍ�Cׄӕ�K�w�v�p՚�rؒ�F�kوٚ�X�J܍�c݈ݜ�w���l��]�o��E���o�W�_�e�}��t�n��g�a�B�G�n��\�G�A�T�F�X�h�c���_�e��h�^�Q�\�_�d�}����E�F�L�K�R�k�v�o�_��Q�H��R��j�L������O�W�N�Q�w�j�f�`�d�~�z�����Y�s�l�[���g�e�F�����@�s�h�g�O�I�{�o�����@�M�A�k���t�������g�O�V�W�^�s�D�L�U�s�{�[�]���a�Uƈ��؂�[�Ȅx���_���r�Y�ʲG�|�S�Y�{�sÏ��N";
    private static byte[][][]   s2tMap        = new byte[256][256][2];
    private static byte[][][]   t2sMap        = new byte[256][256][2];

    /**
     *��ʼ��������GBK����Է�������
     */
    static {
        try {
            { //��ʼ������ת�����

                byte[] buf1 = GBK_T2S_S_MAP.getBytes("GBK");
                byte[] buf2 = GBK_T2S_T_MAP.getBytes("GBK");

                int    len = Math.min(buf1.length, buf2.length);

                for (int i = 0; i < len; i += 2) {
                    t2sMap[buf2[i] & 0xFF][buf2[i + 1] & 0xFF][0]     = buf1[i];
                    t2sMap[buf2[i] & 0xFF][buf2[i + 1] & 0xFF][1]     = buf1[i + 1];
                }
            }

            { //��ʼ������ת�����

                byte[] buf1 = GBK_S2T_T_MAP.getBytes("GBK");
                byte[] buf2 = GBK_S2T_S_MAP.getBytes("GBK");

                int    len = Math.min(buf1.length, buf2.length);

                for (int i = 0; i < len; i += 2) {
                    s2tMap[buf2[i] & 0xFF][buf2[i + 1] & 0xFF][0]     = buf1[i];
                    s2tMap[buf2[i] & 0xFF][buf2[i + 1] & 0xFF][1]     = buf1[i + 1];
                }
            }
        } catch (Exception e) {
        }
    }

    /**
     * ͨ��������õ�������
     * @param hbyte
     * @param lbyte
     * @return
     */
    public static byte[] getS2TValue(byte hbyte, byte lbyte) {
        return s2tMap[hbyte & 0xFF][lbyte & 0xFF];
    }

    /**
     * ͨ��������õ�������
     * @param hbyte
     * @param lbyte
     * @return
     */
    public static byte[] getT2SValue(byte hbyte, byte lbyte) {
        return t2sMap[hbyte & 0xFF][lbyte & 0xFF];
    }

    /**
     * �Ӽ���ת��������
     * @param str
     * @return
     */
    public static String covGBKS2T(String str) {
        return covGBK(str, 0);
    }

    /**
     * �ӷ���ת���ɼ���
     * @param str
     * @return
     */
    public static String covGBKT2S(String str) {
        return covGBK(str, 1);
    }

    /**
     * �ӷ���ת���ɼ��� �� �Ӽ���ת��������
     * @param type
     * @return
     */
    public static String covGBK(String string, int type) {
        if (null == string) {
            return null;
        }

        try {
            byte[][][] m = ((type == 0) ? s2tMap
                                        : t2sMap);
            byte[]     bs = string.getBytes("GBK");

            int        len = bs.length;

            for (int i = 0; i < (len - 1); i++) {
                int b  = bs[i] & 0xFF;
                int b1 = bs[i + 1] & 0xFF;

                //GBK��Χ8140-FEFE
                if ((b >= 0x81) && (b <= 0xFE) && (b1 >= 0x40) && (b1 <= 0xFE) && (b1 != 0x7F)) {
                    byte[] nbs = m[bs[i] & 0xFF][bs[i + 1] & 0xFF];

                    if ((nbs[0] != 0) && (nbs[1] != 0)) {
                        bs[i]         = nbs[0];
                        bs[i + 1]     = nbs[1];
                    }

                    i++;
                }
            }

            return new String(bs, "GBK");
        } catch (Exception e) {

            return string;
        }
    }

    public static long covFileS2T(String fileName, String descFileName) {
        return covFile(fileName, descFileName, 0);
    }

    public static long covFileT2S(String fileName, String descFileName) {
        return covFile(fileName, descFileName, 1);
    }

    public static long covFile(String fileName, String descFileName, int type) {
        long time = 0;

        try {
            StringWriter stringWriter = new StringWriter();
            Reader       in  = new InputStreamReader(new FileInputStream(fileName), "GBK");
            char[]       buf = new char[4096];
            int          len = 0;

            while ((len = in.read(buf)) >= 0) {
                stringWriter.write(buf, 0, len);
            }

            in.close();

            String a = stringWriter.toString();

            long   s1 = System.currentTimeMillis();

            String b = (type == 0) ? covGBKS2T(a)
                                   : covGBKT2S(a);

            time = System.currentTimeMillis() - s1;

            FileOutputStream out = new FileOutputStream(descFileName);

            out.write(b.getBytes("GBK"));
            out.close();
        } catch (Exception e) {
        }

        return time;
    }

    public static void covJspFileInDir(String dir, String toLocale, int type) {
        try {
            File f = new File(dir);

            if (f.isDirectory()) {
                File[] fileList = f.listFiles();

                for (int i = 0; i < fileList.length; i++) {
                    covJspFileInDir(fileList[i].getAbsolutePath(), toLocale, type);
                }
            } else if (f.isFile()) {
                String extName = f.getAbsolutePath().substring(f.getAbsolutePath().lastIndexOf(".")
                        + 1);
                String baseName = f.getAbsolutePath().substring(0,
                        f.getAbsolutePath().lastIndexOf("."));

                int aNameLen = Math.max(f.getAbsolutePath().lastIndexOf("/"),
                        f.getAbsolutePath().lastIndexOf("\\"));

                String aName = f.getAbsolutePath().substring(aNameLen,
                        f.getAbsolutePath().lastIndexOf("."));

                if ((aName.indexOf(toLocale) >= 0) || (aName.indexOf("_en.") >= 0)) {
                    System.out.println("N:" + f.getAbsolutePath());

                    return;
                }

                if ("jsp".equalsIgnoreCase(extName) || "vm".equalsIgnoreCase(extName)
                            || "htm".equalsIgnoreCase(extName) || "xml".equalsIgnoreCase(extName)) {
                    System.out.print("Y:" + f.getAbsolutePath());
                    System.out.print(" ---- ");
                    System.out.print(covFile(f.getAbsolutePath(),
                            baseName + "_" + toLocale + "." + extName, type));
                    System.out.print("ms");
                    System.out.println();
                }
            }
        } catch (Exception e) {
            //e.printStackTrace();
            System.out.println("N:" + dir);
        }
    }

    public static void covPhpFileInDir(String dir, int type) {
        try {
            File f = new File(dir);

            if (f.isDirectory()) {
                File[] fileList = f.listFiles();

                for (int i = 0; i < fileList.length; i++) {
                    covPhpFileInDir(fileList[i].getAbsolutePath(), type);
                }
            } else if (f.isFile()) {
                String extName = f.getAbsolutePath().substring(f.getAbsolutePath().lastIndexOf(".")
                        + 1);

   
                if ("php".equalsIgnoreCase(extName) || "html".equalsIgnoreCase(extName)) {
                    System.out.print("Y:" + f.getAbsolutePath());
                    System.out.print(" ---- ");
                    System.out.print(covFile(f.getAbsolutePath(), f.getAbsolutePath(), type));
                    System.out.print("ms");
                    System.out.println();
                }
            }
        } catch (Exception e) {
            System.out.println("N:" + dir);
        }
    }



    /**
     * ���
     * @param args
     */
    public static void main(String[] args) {

    }

    /**
     * ���ֽ�����ӡ����
     * @param b
     * @return
     */
    private static String bytesToHexStr(byte[] b) {
        if (b == null) {
            return "";
        }

        StringBuffer strBuffer = new StringBuffer();

        for (int i = 0; i < b.length; i++) {
            strBuffer.append(Integer.toHexString(b[i] & 0xff));

            //strBuffer.append(" ");
        }

        return strBuffer.toString();
    }
    
    /**
     * �ж�һ��
     * @param str
     * @return
     */
    public static boolean isInT2SMap(String str) {
        if (GBK_T2S_T_MAP.indexOf(str) == -1) {
            return false;
        } else {
            return true;
        }
    }
}
