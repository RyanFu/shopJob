/*
 * Created on 2004-11-14
 */
package org.dueam.hadoop.utils;

import java.io.*;


/**
 * @author dogun
 */
public class GBKMap {
    private static final String GBK_T2S_T_MAP = "GKyƒ†¸ÐÑ×ãíö‚H‚I‚R‚S‚b‚t‚z‚}‚€‚ƒ‚†‚‚‚¥‚È‚É‚Ê‚Ì‚Î‚Ü‚á‚ã‚ä‚å‚í‚ò‚ô‚÷‚ø‚ù‚ûƒAƒEƒHƒLƒSƒWƒ^ƒeƒfƒlƒrƒxƒzƒ|ƒ}ƒ~ƒ€ƒ†ƒ‰ƒŠƒƒ”ƒžƒ¦ƒ«ƒ¬ƒ­ƒ®ƒ¯ƒ°ƒ´ƒ¶ƒºƒ¼ƒÈƒÉƒÔƒçƒòƒö„C„E„I„P„R„e„h„q„t„w„}„‚„ƒ„Ž„’„“„•„„ž„¡„¢„£„¥„¦„©„ª„Å„Ó„Ô„Õ„×„Ù„Ú„Ý„Þ„à„ã„ê„ì„î„ñ„ò…Q…R…T…U…V…X…^…f…r…s…‡…ˆ…‹……’…”…–…˜…¢…²…Ç…Î†J†T†U†h†r†w†ˆ†–†™†›†¡†¢†¤†¥†¹†¾†Ç†Ê†Ë†Ì†Î†Ñ†Ü†Ý†ß†á†è†î†ô‡@‡D‡I‡K‡L‡O‡W‡Z‡[‡\‡^‡`‡c‡f‡j‡u‡z‡}‡‡‚‡†‡ˆ‡Š‡‡Ž‡“‡˜‡‡Ÿ‡£‡§‡²‡³‡µ‡»‡¾‡¿‡À‡Â‡Æ‡Ê‡Ë‡Ì‡Ï‡Ò‡Ó‡Õ‡Ú‡ß‡è‡é‡ë‡÷‡ø‡úˆ@ˆAˆDˆFˆsˆˆ¢ˆºˆÆˆÌˆÔˆ×ˆßˆåˆêˆòˆóˆö‰A‰K‰L‰N‰P‰T‰V‰]‰_‰j‰m‰q‰t‰|‰‹‰Œ‰™‰›‰ž‰¡‰¦‰¨‰¯‰³‰¶‰º‰¾‰¿‰À‰Â‰Ä‰Å‰Æ‰È‰Î‰Ï‰Ñ‰Ø‰Ú‰Û‰Þ‰ò‰ôŠAŠJŠWŠYŠZŠ^ŠyŠ™Š¦Š©ŠÊŠÕŠä‹D‹H‹I‹z‹‹‚‹‹‹Œ‹–‹ž‹³‹¸‹¹‹½‹¾‹Æ‹È‹É‹Ô‹Ø‹Ü‹ß‹å‹è‹ë‹ð‹ö‹úŒDŒOŒWŒ\ŒmŒ‹ŒŒŽŒŒ‘Œ’Œ™ŒšŒ¡Œ¢Œ£Œ¤Œ¦Œ§Œ¼ŒÀŒÃŒÆŒÈŒÏŒÒŒÓŒÕŒÙŒÚŒùsu{ˆŠ‹‘–˜ž£¹Àâäçô÷þŽAŽFŽGŽMŽSŽVŽXŽZŽ[ŽhŽnŽpŽrŽ€Ž„Ž›ŽŸŽ¤Ž§Ž¬Ž®Ž½Ž¾ŽÃŽÅŽÍŽÎŽÓŽÖŽ×ŽìŽúŽûŽýBFJNPRSTUV[]dhit†ˆŠ•—™›¡¤¥§©¬·½ÄÆÍÏÕØau‚ž¢ºÀÁÅÛÜâíð÷ü‘@‘B‘C‘K‘L‘M‘Q‘T‘U‘Y‘Z‘]‘a‘b‘c‘h‘j‘n‘v‘z‘{‘|‘€‘„‘‘‘‘“‘—‘›‘©‘ª‘«‘¬‘±‘º‘»‘¿‘Ã‘Í‘Ð‘Ñ‘Ò‘Ô‘Ö‘×‘Ø‘Ù‘ß‘â‘ê‘ì‘ï‘ð‘ò‘ô’L’’¶’Î’Ð’Ô’ß’à’é’ê’ì’ñ’þ“P“Q“]“d“k“l“p“u“v“Œ“’““¥“§“«“´“¸“»“½“Æ“Í“Î“Ï“Ó“×“Û“Ü“á“ä“å“é“ë“ì“í“ï“ñ“ô“õ“ú“û“þ”D”E”F”H”M”P”Q”R”S”U”X”[”\”]”_”d”f”n”r”t”v”x”y”z”€””‚”†”‡”ˆ”Ž”›”¡”¢”®”³”µ”·”¿”À”Ì”Ø”à”ç•N•r•x•ƒ•ž•Ÿ•ª•³•º•¿•Ï•Ñ•Ò•Ô•Ú•á•ç•è•î•ñ•ø•þ–V–|–¡–Å–Ê—U—d—g—l—n—y—‰—–————£—¨—«—®—¿—î—÷˜E˜I˜O˜o˜q˜s˜˜„˜‹˜Œ˜˜ ˜¡˜ª˜³˜¶˜·˜º˜Å˜Ç˜Ë˜Ð˜Ó˜â˜ã˜ä˜å˜ï˜ò™C™E™M™R™_™f™n™u™x™z™{™„™…™‰™Ž™‘™”™˜™™™©™°™±™³™´™µ™»™½™¾™À™Á™Â™É™Î™Ð™Ñ™Ú™à™å™ç™è™ì™ôšJšUšWš^šašešgšqšvšwš{šˆšŒš‘š—ššš›šžš¢š¤š§šªš­š¿šÂšÐšÓšÖšØšÚšâšäšåšèšëšïšø›@›A›Q›Z›]›_›r›ª›°›Ñ›Ü›ã›öœDœIœOœQœRœSœYœZœ\œoœpœtœuœyœ†œœœ¡œ¥œ«œ¿œÊœÏœØœÛœÝœáœæœçœìœîœóœûœþBFGILMOUahinqsu{}Š‘“•™¡¢§¬­®¯²³¾ÆÉËÍÒÕáâðñô÷øúýžEžFžHžIžNžRžTžVž]ž^žažcžgžlžožržtžužzž{ž|ž…ž‡ž‘ž–ž—ž¢ž¦ž©ž®ž³ž´ž·ž¹žÄžÝžéžõŸNŸoŸŸŸ’Ÿ˜ŸœŸŸŸ¦Ÿ¨Ÿ©Ÿ¬ŸÉŸÍŸáŸâŸëŸîŸðŸôŸõŸûŸý C F I N S T Z ` a c d q t { € Ž ‘ ” – —    £ © ¹ ¿ Î Ó Ù Þ îªMªNªbªqªsªwªyªzª{ª„ªšªœªªžªŸ«@«C«E«F«H«I«J«M«R«k«˜¬F¬P¬g¬m¬q¬z¬|¬„¬¬Ž¬“¬”¬˜¬š­I­\­^­a­c­h­m­t­v­‚­‡­‹­‘®T®U®Y®Z®a®b®d®€®…®‹®®‘®”® ¯B¯d¯i¯w¯{¯¯‚¯ƒ¯ˆ¯Ž¯¯‘¯”¯›¯œ¯Ÿ°A°B°D°K°O°T°V°W°X°Y°[°]°_°`°a°b°c°d°l°}°’°—°™° ±I±K±M±O±P±R±U±i±x±{±Š±—± ²A²g²m²€²‰²”²š³C³h³p³³ˆ³Œ³Ž³•´T´X´^´_´a´o´u´~´ƒ´‰´“´™µAµKµVµZµ[µ\µ^µaµvµzµ“µœµ¶B¶R¶U¶Y¶[¶\¶d¶i¶¶’¶ ·A·M·N·Q·Y·d·e·f·v·w·x·~·€·‚·„¸C¸D¸F¸G¸H¸M¸Q¸Z¸[¸]¸^¸`¸p¸w¸‚¹P¹S¹a¹w¹{¹~¹Š¹¹ ºBºDºOºSºVºYº`ºjºtºwº„º†ºˆºº™ºšºžºŸ»@»I»L»U»X»[»\»^»`»a»e»f»h»j»n»’»–»›¼R¼S¼U¼Z¼a¼c¼e¼g¼m¼o¼q¼s¼t¼u¼v¼w¼x¼y¼{¼~¼‚¼ƒ¼„¼…¼†¼‡¼ˆ¼‰¼Š¼‹¼Œ¼¼’¼™¼š¼›¼œ¼¼Ÿ½B½C½E½H½I½K½L½M½O½V½W½X½Y½^½d½f½g½j½k½o½q½x½y½z½{½½‰½‹½Ž½½‘½”½—½™½›¾C¾E¾G¾I¾J¾Q¾R¾S¾T¾U¾V¾W¾X¾Y¾Z¾]¾^¾_¾`¾b¾c¾d¾i¾l¾o¾p¾r¾w¾x¾y¾|¾}¾~¾€¾‚¾ƒ¾„¾†¾‡¾‰¾Š¾Œ¾Ž¾¾’¾•¾—¾˜¾š¾œ¾Ÿ¿@¿A¿G¿M¿N¿O¿P¿U¿V¿Z¿\¿_¿`¿b¿c¿d¿h¿l¿p¿r¿s¿t¿v¿w¿y¿z¿{¿|¿~¿‚¿ƒ¿‡¿‰¿Š¿‹¿•¿—¿˜¿™¿œ¿À@ÀCÀDÀHÀKÀLÀMÀOÀPÀQÀRÀUÀ[À^À_À`ÀhÀiÀkÀmÀnÀpÀtÀwÀyÀ|ÀÀ—ÀšÀ›ÀÀžÁPÁRÁTÁUÁ_Á`ÁbÁdÁsÁuÁwÁxÁƒÁ•ÂEÂNÂOÂPÂZÂaÂeÂgÂ}Â„Â“Â”Â•Â–Â˜Â™ÂšÂœÂŸÂ Ã@ÃCÃ{Ã|Ã}Ã„Ã‹Ã‘Ã“Ã›ÄIÄLÄTÄXÄ[Ä_ÄcÄdÄeÄsÄwÄzÄÄ‘Ä’Ä“Ä˜ÄšÄœÄŸÅDÅFÅKÅLÅNÅPÅRÅVÅ_ÅcÅdÅeÅfÅmÅoÅ“ÅšÅ›ÅœÅžÆ@ÆAÆDÆGÆHÆcÆrÆÇGÇfÇoÇvÇ{ÈAÈCÈOÈRÈfÈnÈ~È‡ÈÈ’È”È™ÈÉLÉOÉPÉWÉnÉpÉtÉwÉ†É‰ÉÉÉ”ÉœÊCÊNÊVÊYÊ[Ê\ÊaÊhÊnÊrÊwÊ{Ê|Ê~ÊÊ‰ÊŽÊÊ’ÊšËCËEËGËKËNËRËWË]Ë_ËjË{Ë|Ë‡ËŽË’ËžÌ@ÌAÌEÌIÌJÌKÌNÌOÌ\Ì`ÌdÌmÌyÌ}ÌŽÌ“Ì”Ì–ÌÍAÍÍ‘Í˜ÎgÎoÎrÎtÎÎ‡Î•Î›ÎžÏNÏQÏUÏXÏ\ÏlÏsÏuÏxÏ|ÏÏ‰ÏŠÏÏ“Ï”Ï–ÏžÏ ÐDÐMÐQÐUÐ\Ð`ÐgÐkÐlÐnÐoÐ}Ð–ÑUÑWÑYÑaÑbÑeÑuÑ}Ñ‚Ñ‹ÑÑžÒ@ÒCÒLÒMÒSÒUÒ\ÒcÒdÒhÒmÒrÒuÒwÒ‡ÒŠÒÒŽÒ’Ò“Ò•Ò—Ò›Ò ÓCÓDÓHÓJÓMÓNÓPÓUÓXÓYÓ[Ó]Ó^ÓhÓxÓzÓ|Ó…Ó†Ó‡Ó‹ÓÓÓ‘Ó’Ó“Ó•Ó–Ó˜Ó™ÓšÓ›ÓžÓ ÔAÔDÔEÔGÔHÔKÔLÔOÔSÔVÔXÔ\Ô]Ô^ÔbÔgÔnÔpÔrÔtÔuÔvÔwÔxÔ{Ô~ÔÔ‚ÔƒÔ„Ô‡ÔŠÔŒÔÔŽÔÔ‘Ô’Ô“Ô”Ô–ÔœÔžÔŸÕCÕDÕEÕFÕIÕJÕNÕOÕQÕTÕVÕZÕ\Õ]Õ_Õ`ÕaÕbÕdÕfÕhÕlÕnÕrÕuÕxÕ{Õ~ÕÕ„Õ†ÕˆÕŠÕŒÕŽÕÕ“Õ”Õ˜Õ™ÕšÕ›ÕžÕŸÖ@ÖBÖCÖGÖIÖJÖMÖOÖRÖSÖTÖVÖXÖZÖ\Ö]Ö^Ö`ÖaÖeÖiÖkÖoÖqÖrÖtÖuÖvÖxÖ{ÖƒÖ†Ö‡ÖŽÖ”Ö™Öœ×@×C×F×H×I×P×R×S×T×V×Y×d×e×g×h×l×o×p×u×v×x×y×}×ƒ×„×…×‡×‰×‹×Œ×Ž××“×”×•×—ØGØMØQØSØWØiØrØ‚ØˆØØ‘Ø’Ø“Ø”Ø•ØšØ›ØœØØžØŸÙAÙBÙDÙEÙFÙHÙIÙJÙLÙMÙNÙOÙQÙRÙSÙTÙUÙVÙWÙYÙZÙ[Ù\Ù_ÙcÙdÙeÙfÙgÙkÙlÙmÙnÙpÙrÙsÙtÙuÙvÙxÙyÙ|Ù}Ù~Ù€ÙÙ‡ÙˆÙ‹ÙÙŽÙÙÙ‘Ù—Ù˜ÙšÙ›ÙÙžÙ ÚAÚBÚEÚFÚHÚIÚMÚNÚXÚsÚwÚ…ÚŽÛEÛRÛ`ÛmÛxÛ„Û‹Û”Û•Û˜Û™ÜEÜJÜOÜPÜQÜSÜUÜVÜWÜXÜ]ÜbÜfÜgÜkÜnÜ|Ü€Ü‡ÜˆÜ‰ÜŠÜÜŽÜÜ—Ü›Ü ÝFÝMÝSÝTÝUÝVÝWÝYÝ^Ý`ÝbÝcÝdÝeÝmÝnÝoÝpÝvÝwÝxÝyÝzÝÝ‚Ý…Ý†ÝˆÝ‰Ý‹ÝÝ”Ý—Ý˜ÝšÝ›ÝœÝžÝ Þ@ÞAÞDÞHÞIÞOÞZÞ\Þ]Þ_ÞiÞkÞoÞpÞqÞrÞsÞ~Þ’Þ•ÞŸß@ßBßLßMß[ß\ß^ß_ß`ßbßdßeßfßhßmßtßwßxßzß|ß~ß€ßƒß…ß‰ßŠàAàPàSà]àiàlàuàwàxàyà{à‡àààà’à”à—áBáOáZádáhájátáuáwá~á€á„á…á‡á‰áŒáááá‘á“á”á•á˜ážáŸâ@âAâCâFâOâQâSâTâZâ[â]â^â_âbâcâgâhâjâkâlânâoâxâ{â}â€ââ‚â‰â‹âŽââ’â“â”â•â˜â™âšâ›âŸâ ã@ãBãCãEãGãKãOãQãTãUãXã\ã^ã`ãfãgãlãoãqãsãtãxãyã|ã~ã‡ãŠãŒãŽãã‘ã“ã”ã•ã™ãœãžãŸä@äAäBäCäDäHäIäJäNäPäRäSäXäYäZä\ä^äbäeähäoäräsäuäyäzä{ä|ä~ä€ää…ä†ä‡äˆäää’ä“ä˜ä›ääžäŸåEåFåHåKåNåOåPåQåUåVåWåXå\å^å_åaådåeåiålånåråuåvåxå{å|å}å€ååƒåŠåŽåå‘å–åšå›åŸå æ@æDæIæJæNæPæRæVæXæ[æ^æ_ægæiækæmænætæuævæyæzæ|æ}æ€æ‚æ„æ†æ‡æ‰æŒæ“æ—æ›æœæŸæ çBçCçHçIçKçMçNçOçPçRçSçUçYçaçfçhçjçnçtç|ç‚ç„ç…ç†çŠç‹çç’ç˜ç™çšçç èCèDèFèGèIèKèOèTèZè\èaèbèdèeèkènèpèsètèuèzè|è€èè‚è‡è‰èŒèèŽèè‘è’éLéTéVéWéZé\é]é_é`ébécéeéfégéhéléméuévéwéxéyé|é}é€éé‚é†é‡é‹éŽéé‘é’é“é”é˜é›ééŸé ê@êAêDêFêGêHêIêJêNêPêRêTêUêVêXêYêlênê€ê„ê…ê‡êŽêê‘ê–ê›êêŸê ëAëEëFëHëOëSëUë[ë]ë_ë`ëbëhëmëpërësëuëxëyë…ëŠë•ëìCìFìVìZì\ì^ì`ìaìnìoìrìtìvìzìŠì’ì–íFíXí\í^íaídífígíhínítíwíxíyí€íí‘í“í”í•í—í˜í™íšíœíží î@îAîBîCîDîHîIîMîRîUîWî\î^î_îaîcîeîhîiîjîlînîwî}î~î€îî„î…î†îŠî‹îîî”î—î™îîžï@ïAïBïDïEïLïQïRïSïUïWïZï\ï^ï_ï`ïdïgïhïjïkïlïwï|ï}ï€ï‚ïƒï„ï†ïˆï‹ïï•ï–ï—ï˜ïœïïžðAðBðDðEðFðGðHðIðKðLðNðPðQðRðTðWð^ðfðhðjðkðlðoðqðrðsðtðvðxðzð}ð~ð€ðð‚ð‡ðˆð‹ðð‘ð’ð–ñRñSñTñWñYñZñ_ñgñvñwñxñzñ{ñ~ñ€ñ‚ñ„ñ†ñˆñ‰ñ”ñ•ñ—ñ˜ñŸòEòGòHòKòRòSòTòUòVò\ò]ò_òiòjòqòsòtòvò|ò}ò~ò…ò‡òˆò‰òŠò‹òŒò‘ò“ò”ò–òœòžòŸó@óAóEóHóJóKóLóPóQóiótóvówóxóyó‰óŒóœó ôEôPôWôYôZô[ô\ô]ôaôbôdôuô|ô~ô€ô‡ôô”ô™ôœôŸõEõGõNõOõPõQõRõTõUõVõWõ^õ`õbõjõnõoõqõrõwõzõ{õ~õ…õ†õŒõŽõõ—õ™õšõ›õœõ öAöEöFöHöKöLöNöOöTöXöYö[öaöcöeöfögölömöpöqösötövöwö€öö„ö…öˆöŠööŽö’ö“ö–ö—ö˜öšöœöžöŸö ÷@÷B÷F÷I÷L÷M÷V÷W÷X÷Z÷[÷\÷a÷c÷d÷g÷h÷k÷l÷q÷s÷{÷|÷~øBøDøFøOøPøQøSøcødøføoørøxøzø{ø|øø„ø†øŠøøŽø’ø™øø ù@ùMùNùOùPùYùZù[ù]ù^ùgùiùkùlùoùsùtù{ù…ù‡ùˆù‘ù”ù–ù˜ùŸúAúBúFúIúLúOúQúVúWúXúYú\ú]ú^ú_úaúgúpúsútúvúwúƒú„ú‰úúŽúú‘ú’ú–ú—ú˜úšûDûIûKûLûRûUûWûXûZû[ûuûyûzû|û}ûƒûûœûŸû üDüIüNüSüZücühüoüqüsütüwüxü{üü‚üƒüŠýBýOýRýSýVýWýXýZý[ý]ý_ýbýeýfýgýiýlýmýpýrýxý|ý}ýˆý‹ýýý”³ÝƒJƒÙƒô„x„Á…¼…È…Õ‰„Š…ŒÐŒý»ŽmŽÙbs‘à’A’K’M’W’\’^’d’w’±“{“Ú•„•Å–X–g–•š¯š÷›É ²­®Œ¯X¯q¯r°š²G²[²tµnµo¹H¹uº´¿xÀuÃÆSÆˆË^ËŸÌYÒoÓbÖøÛsæjçzèNêiêƒóaúNþ@";
    private static final String GBK_T2S_S_MAP = "¶ª²¢ÂÒØ¨ÑÇ·òØù²¼Õ¼²¢À´ÂØÂÂ¾ÖÙ¶ÏµÏÀØöÁ©²Ö¸öÃÇÐÒ·ÂÂ×Î°²àÕìÍµÔÛÎ±½ÜØ÷É¡±¸Ð§¼ÒÓ¶ÙÌ´«ØñÕ®ÉËÇãÙÍ½öÙÝÇÈÆÍÎ±½ÄÙÇ¹Í¼ÛÒÇÙ¯ÒÚµ±¿ë¼óÙÏÙ±Ù­¾¡³¥ÓÅ´¢Ù³ÂÞÔÜÙÐÙÎÙ²Ð×¶Ò¶ùÙðÄÚÁ½²áÃÝÍ¿¶³ÁÝäÂ´¦¿­Æ¾±ðÉ¾ØÙÔò¿Ë„i¸Õ°þ¹ÐØÜ´´²ù»®Ôý¾çÁõ¹ôØÛ½£¼Á½£¾¢¶¯ÛÃÎñÑ«Ê¤ÀÍÊÆ¼¨½Ë„ÖÛ½Ñ«ÀøÈ°ÔÈØÐ»ãØÑÞÆÞÆèüÇøÐ­ÐôÈ´ØÇÌü²ÞÀúÑá³§À÷ØÉ²Î´ÔÎâÂÀßÃÔ±ßÇßÂßðßÄÄîÎÊÆôà¢ÑÆÆô†|ÏÎÔÛ»½ÑÒÉ¥³ÔÇÇµ¥Ó´ÇºØÄ†yÂðÎØßïßÙÌ¾à¶Å»ßõ³¢ßé»©ßëÐ¥ß´ßØß¼†®¶ñßÔÐêßÐßÕßæßÜàÈßàÅç¶Öµ±ßÌÏÅßâ³¢ààÄöÑÊß¿ÁüÏòà·ÑÏàÓÏùßùà¿ÏùÙæß½†ªËÕÖö»Ø´Ñ»Ø¹úàð¹úÎ§Ô°Ô²Í¼ÍÅÛðÛû°ÓÛë²ÉÖ´¼áÛÑÛñÛö½×Ò¢±¨³¡¼î¿éÜãÛîÛõÍ¿Ú£ÎëÛ÷³¡³¾Çµ×©µæ×¹íÍ¶éÌ³·Øˆ™Ç½¿ÑÌ³ˆ›Û÷Ñ¹ÀÝÛÛÛäÌ³»µÂ¢Â¢ÛÞ°Ó‰G×³ºø‰×ÊÙÊÙ¹»ÃÎ¼ÐÛ¼°ÂÞÆ¶á·Ü×±æ©¼éÖ¶ÓéåüÂ¦¸¾Òùæ«æ´Íµæ£æÁÂèôÁåýåüæµæµ‹Oæ£æ¬æ¿½¿æÍôÁæÈæÖæÉÄÌÓ¤ÉôÀÁÄïæ®ËïÑ§ÂÏ¹¬ÇÞÊµÄþÉóÐ´¿í³è±¦¿Ë½«×¨Ñ°¶Ôµ¼ÞÏÞÏ½ìÊ¬ŒÁÌëÂÅ²ãåðÊôŒÁ¸Ôá­µºÏ¿áÁÀ¥À¥¸ÚÂØá´á¿á´áÎá°ËêáÐÕ¸á«ÂáÀá½iá»Ná®áÉÁëÓìÔÀ¿ùÂÍáÛÑÒÛÏÚáË§Ê¦ÕÊ´øÖ¡àøàþàýÖÄ±Ò°ïàüÒ[¸É¼¸¿â²ÞÏá¾ÇÏÃŽöÒñ³øØËÃí³§âÐ·Ï¹ãâÞÂ®Ìü»ØÄËµõåòÕÅÇ¿±ðµ¯Ç¿ÃÖÍä»ãÒÍÒÍ¦Ñåµñ·ð¾¶´Óáâ¸´·ÂÕ÷³¹ºã³ÜÔÃâêÃÆÆà¶ñÄÕã¢âü°®ã«í¨âëâýâéÀõÒóÌ¬ã³²Ò²Ñ²Ñâú¹ßí¨âæËËÂÇã¥ÉåÇìÆÝÓûÓÇ±¹Á¯Æ¾ã´‘\µ¬·ßÃõâäÏÜÒä¿ÒÓ¦âøãÁâûÃÉí¡í¯âû³ÍÀÁ»³Ðüâã¾å»¶ÉåÁµí°ê§ê¨ê¯Ï·Õ½Ï·»§Ç¤Å×Ð®ÉáÞÑ¾íÉ¨ÂÕ’¥Õõ¹Ò²É¼ðÑï»»»Ó±³¹¹ÞìËðÒ¡µ·ÇÀÕ¥ÞâÞèÂ§ÞêÖ¿¿ÙÞÒ²ôÀÌ’¦³ÅÄÓÄíÞØµ§²¦¸§ÆËÞìÌ¢ÎÎ¼ñÓµÂ°Ôñ»÷µ²µ£Ð¯¾Ý¼·Ì§µ·¾ÙÄâ±÷Å¡¸éÖÀÀ©ß¢°ÚËÓß£ÈÅÞóÄìÂ£À¹Þü²óß¥Ð¯ÉãÔÜÂÎÌ¯µ²½ÁÀ¿¿¼Ðð°ÜÐðÑïµÐÊýÇýÁ²±ÐìµÕ¶¶ÏÆìÉýÊ±½úÖçÔÎêÍ•D³©ÔÝêÇêÊÀúê¼ÏþÏòêÓ¿õµþ•oÉ¹Êé»áëÊ¶«¹ÕÕ¤¹Õ¸ËèÙèÅÌõèÉÀ¦ÆúèÇÔæ¶°Õ»èðÆÜ—…èâÑî·ãèåÒµ¼«¸Éè¿ÈÙèçÅÌ¹¹Ç¹¸Üèýé¤½°¹æ×®ÀÖèÈÁºÂ¥±êÊàÑù´ÔÆÓÊ÷èëèãÇÅ»úÍÖºáéÀéÝèßµµèí˜–¼ìéÉ—ƒÌ¨éÄÄû¼÷ÜÜèþ¹ñéÖéµèÎèüéÚèÝ³÷éÆèÓèÀéÍé´èÐé·èùÓ£À¸È¨é¡ÔÜèïé­èùÇÕÌ¾Å·Ð¥Á²ì£»¶ËêÀú¹ééâ²Ðéæéäéééçéë¼ßÉ±¿Ç»ÙÅ¹Ò½ÈÞÇòë§êóÕ±Õ±ëªÆøÇâë²ëµÛÊ·º·ºÎÛÎÛ¾öÙüÃ»³å¿öÐ¹ÐÚä¤ãþÝ°Á¹ÆàÀáäË¾»ÁèÂÙÔ¨äµÇ³»Á¼õ›hÎÐ²â»ë´Õä¥íªÓ¿ÌÀãí×¼¹µÎÂ›¸›éÊª²×ÃðµÓÜþ»ã»¦ÖÍÉøÂ±ä°›º¹öÂúÓæœ¾Å½ººÁ°×ÕÕÇäÓ½¥½¬ò£ÆÃ½àãíÇ±ôªÈóä±À£äää¶É¬É¬³Î½½ÀÔ½§äÅÔóœùí´ä«µí×ÇÅ¨›mÊªÅ¢ÃÉ›»¼ÃÌÎÀÄ¿£Î«±õÀ«½¦ãøÂËäÞäÂÐºäÉä¯±ôãòÁ¤äìäëäóãñäþÃÖäòÀ½ããäÜÈ÷ÀìÌ²å°ÍåÂÐäÙäÙÔÖÕÕÎªÎÚÌþÎÞìÑ»ÔÁ¶ì¿Å¯ÑÌÜä»À·³ì¾Ó«ìÁÈÈïG³ãìÇÑæµÆìÀÁ×ÉÕÌÌìËÓª²Ó»ÙÖò»âÑ¬½ýìâÒ«Ë¸Â¯ìÇÀÃÕùÎªÒ¯¶û´²Ç½¼ãÕ¢ë¹µÖÇ£Üýêó¶¿Îþ×´ÏÁ±·ÕøÓÌáøáï´ôÓüÊ¨½±¶ÀáöáýªAÄü»ñÁÔáîÊÞÌ¡Ï×â¨â¤×ÈçåÅåÏÖÁ§¹Ü·©çõçëçâ«`ËöÑþÓ¨ÂêÀÅ«oçö¬Qçáè¨«š»·«_çôè¯Çíççè¬è¶ê±×©ÎÍó¿²ú²úËÕÄ¶±Ï»­ÒìÁôµ±³ëµü¾·Ëá±ÔðéÓú·èÑñ»¾ðù´¯Å±¯}ðüðüÁÆðìðïð÷ÓúðÝ±ñ³ÕÑ÷ðÜÖ¢ðßñ®Ñ¢ñ¨ñ«Ó¸Ì±ñ²·¢°¨ðåñäÖå±­µÁÕµ¾¡¼àÅÌÂ¬µ´ÊÓÊÓíöÖÚÀ§ÕöíùíîÂ÷íúÃÉ±€Öõ½ÃÅÚÖì³níÌíºÑâí×Ë¶í¸í¿È·Âë³}×©í×íÓí¶íÍ³~´¡°­¿óíÂÀù·¯ÅÚíÃÓÓÃØÂ»»öìõµtÓùìøÀñìòµ»ÍºôÌË°¸ÑÀâÙ÷½ÕÖÖ³Æ¹ÈöÕ»ýÓ±¶Œð£»àÍÇÎÈ»ñïùÎÑÍÝÇîÒ¤Ò¤ñÀ¿ú´ÜÇÏñ¼ÔîÇÔ²¢Êú¾º±ÊËñóÈ¸ö¼ãóÝé¢½Ú·¶ÖþóæóèóãóÆÉ¸óÙóåÂ¨Ëòóì¼òóñóïéÜ¹YÇ©Á±Àº³ïÌÙ¹‚óêô¥ÁýÞÆÇ©Ô¿óÖóýÀéÂáÓõ×±ÇúÔÁôÖ·àÄ£Á¸ÍÅôÏÙáôÐ¾À¼ÍæûÔ¼ºìæúæüæýÈÒÎÆÄÉÅ¦ç£´¿ç¢ÀƒÉ´À€Ö½¼¶·×ç¡À·ÄÔúÔúÏ¸ç¦ç¥ÉðÀ‚ÉÜç¤ç¨çªç©ÖÕÏÒ×é°íÀç¬ç¥½á¾øÌÐç«½ÊÂçÑ¤¸øÈÞÀ„Í³Ë¿ç­¾î°óç¯ç®ç°ÐåÀ…ËçÀ¦¾­×Ûç¶ÂÌ³ñç¹Ïßç·Î¬À‡çº¸ÙÍø±Á×º²ÊÂÚç¸ç²ÕÀ´Âç±Ãàçµç»½ôç³çÅÐ÷À†ç´ç½¼êç¼ÏßÃà¼©¶ÐµÞçÅÔµç¥çÁ±à»ºÃåÎ³çÃç¿Á·çÂç¾ÖÂÀˆ×ÜÝÓçÆçËçÄç§çÌÀˆÀŠÌÐ¸¿çÇçÉçÈÏØÌÐ·ìçÊËõÑÝ×ÝçÐÏËçÏôêÂÆçÎ×Ü¼¨±ÁçÒçÑñßçÕÖ¯ÉÉÉ¡·­çÔÈÆÐåçÀñßÉþ»æÏµ¼ëçÖçÙçØ½ÉÒï¼ÌçÍç×ïKçÓæþÐøÀÛ²øÓ§ÏËçÚÀÂ²§Ì³ÎÍó¿Ì³Ûä·£Âî°Õ·£ÂÞî¼î¿ØÂÈÞôÇÏÛÒåëþÏ°ÁšÇÌ°¿Á™¶Ë³úñïñìÊ¥ÎÅÁª´ÏÉùËÊñùÄôÖ°ñ÷ÌýÌýÁûËàÐ²Ð²ÂöëÖ´½ÐÞÍÑÕÍÉöëËëáÄÔÖ×½Å³¦ëÉëðÄN·ô½ºÄåµ¨ëÚÅ§Á³Æêë÷ñ³À°ëÍÔàÙõÅHÎÔÁÙ¸ÞÌ¨ÓëÐË¾Ù¾ÉÆÌ¹Ý²ÕéÉéÖô¯½¢éÖôµ¼èÑÞÜ³Û»ÜÑ×È¾£×¯¾¥¼ÔÜÈ»ªâÖÜÉÀ³ÍòÝ«Ò¶Ý¦²ÎÝ§Î­Ò©»çËÑÝ»ÝªÝ°²ÔÝ¥Ï¯¸Ç²ÎÝ¯Á«ÜÊÝ»ÜêÁâ²·Ýä½¯´ÐÜàÒñÂéÝ¡ÝÛÜñÝ¤Ü¿ÝµÜéÝÞµ´ÎßÏôÝ÷Üö¼»Ü¼½ªÇ¾ÇQÝ²¼öÈøÜùÀ¶Ý£ÒÕÒ©Þ´ÜÂ°ªÝþÈ[Þ­Â«ËÕÔÌÆ»ÞºÝüÜ×À¼ÝñÂÜ´¦ÐéÂ²ºÅ¿÷ò°òÌÍÉò¹Ê´â¬ÏºÊ­ÎÏòÏÒÏÂìÓ©ò÷Î…ÕÝòåòýò±²õòÍ³æòÉÒÏÓ¬ò²Ð«òÓòîòºÀ¯òÃÏ]¹Æ²ÏÂùÖÚÃïÊõºúÎÀ³åÎÀÖ»ÙòôÁ¼ÐÀï²¹×°ÀïÖÆ¸´ÑTÐ„¿ãñÍñÚÙôá¥ñÐÔÓÑB°ÀñÏñÉñÜÍà³ÄÏ®Ò[ºË¼ûÓ_¹æÃÙÃÙÊÓêèÌ÷êêÓ`êìÇ×êéêíêïêîêï¾õêïÀÀêë¹ÛµÖõüö£´¥Ú¥¶©¸¼¼ÆÑ¶Ú§ÌÖÓõÚ¦×šÑµÚ¨ÆýÍÐ¼Ç¶ïÑÈËÏÐÀ¾÷Ú«ÚÈ×›·ÃÉèÐíËßÚ­Õï×¢Ö¤Ú¬Ú®ÚªÕ©Ú±Ú¯ÆÀ××œÚ°×ç´ÊÓ½Ú¼Ñ¯ÒèÊÔÊ«²ïÚ¸¹îÚ¹Úµ»°¸ÃÏêÚ·Ú¶×›Ú´Ú³ÖïÚ²¿äÖ¾ÈÏÚ¿ÚÀµ®ÓÕÚ½Óï³Ï½ëÎÜÎóÚ¾ËÐ»åËµËµË­¿ÎÚÇ·ÌÒêµ÷ÚÆ×»Ì¸ÚÃÇëÚºÚÁÚÂÁÂÂÛÚÅÚÄµý× ÚÒÚÖÚ»ÚÌÚÐÐ³ÚÉÚÍÚÑ»äÚÏÚÈ·íÖîÑèÚÎÅµÄ±ÚËÎ½ÌÜÖß»ÑÃÕÚ×ÚÊÚÕ°ùÇ«ÚÖ½²Ð»Ò¥ÚÓÚØÃýÚ©½÷Ã¡»©ÎûÖ¤¶ïÚÜ¼¥ÚÚÊ¶ÚÛÌ·Æ×ÔëÚÞ»ÙÒëÒéÇ´»¤×žÓþÚÙ¶ÁÚØÉó±äÔ€ÑàöÅöÅ²÷ÈÃÀ¾ÚßÔÞß½ÚÔÚÝÏªÆñÊú·áÑÞÖíØkÀêÃ¨±´ÕêÚO¸º²Æ¹±Æ¶»õ··Ì°¹áÔðÖüêÛêß·¡¹ó±áÂò´ûêÜ·ÑÌùêÝÃ³ºØêÚÂ¸ÁÞ»ßêà×Ê¼ÖÐôÔôÔßêâÉÞ±ö±öêäÚQêãÔÞ´ÍÉÍÅââÙÏÍÂô¼ú¸³êæÖÊêåÕË¶ÄêáÀµÚRÊ£×¬êç¹ºÈüØÓêÞ×¸ÚSÔùÔÞØÍÉÄÓ®êáÔßÚPÊêØÍ¸ÓÔßÚW¸ÏÕÔÇ÷ôõ¼£¾Ö¼ùòéÓ»õÄõÏ¼£õÅõç×ÙõÎÛQõ»³ìõÒÔ¾õÜõÈõÙõéõÑõæ´ÚõòõïÌåÇû³µÔþ¹ì¾üÞaÐùéíéîÈíéõéôéïÖáéòé÷éðéóéø½ÏéûéúÞbÔØéùéüÍì¸¨ÇáÁ¾ê¢»Ôéþê¡¹õéý±²ÂÖÞcÈí¼­ê£Êä·øÞdÕ·ÓßÞdì±Ï½Ô¯ê¤×ªÕÞ½Îê¥ºäàÎéöéñ´Ç°ì´Ç±è±çÅ©Å©åÆ»ØÄË¾¶ÕâÁ¬ÖÜ½øÓÎÔË¹ý´ïÎ¥Ò£Ñ·í³µÝÔ¶ÊÊ³ÙÇ¨Ñ¡ÒÅÁÉÂõ»¹åÇ±ßÂßåÎºÏÛ£Û§ÓÊÛ©Ïç×ÞÚùÏçÔÇÜ­µËÖ£ÁÚµ¦ÚþÛ¦Ú÷Ûª³êëçÔÍ³óÔÍÒ½½´áN³êÑàÄðÐÆõ§õ¦ÊÍÀåîÅîÆîÉîÈ¶¤îÇÕëµöîÌ¿ÛîË·°º¸îÎîÊîÏÇ¥îÙîÕè—î×Ô¿îÐÄÆ¶Û¹³îÔîÓè•³®Å¥¾ûÖÓ¸ÆîØîÑîÖîêîæîÝÁåîÜîàîëîÚîßÓËîä¼ØÌú¾Þ×êîèîçÅÙîé²¬îÞÇ¯Ã­Ç¦îá²§¹³îÛîâîãï´è™½Âîï¸õîþÒøï¥Í­èœÏ³îýÌúîùÃúï¢èÏÎîîï¨Ò¿î÷ï§îûîðï¤îíîöº¸ÈñÏúÐâÌàï±ÂÁïÑï¶Ð¿±µîúîò·æèžïíï²ï·îô³úï­ï¸ï°èšÆÌîñï¯ï®ï«ï³¾â¼ø¸Öï¾Â¼ïºïÂïÃè›×¶ï¹ï¿´¸ïÅï£ï¼ïÄ¶§èŸÇ®½õÃªè ÎýïÀ´íÃÌ±íïªïÕï½ÏÇïÁîÍïÇéAÁ¶¹ø¶ÆïÉÕ¡è–¶ÍéBïÊïÆï»ÇÂÇÂïÌ¼üïÈÕàÕëïñÃ¾ïÍïÑ°÷Ï½éFËøïÓ´¸éDéCÎÙÝöïÖîøï¡ïË¸äÕòï×éEÄøïØïÔïÕïßÐýÁ´ïÒïÝéHïáï¬ïÏÆÝïÛïÜïÞ²ù¾µïÚïÎöÉéGîüïäïêÐâîóï¦ÁÍï©ïæïâÖÓïëïãïèï´ïµïÐïÔÁ­ïíÀØÌúéIîìîõïîÖýïìïÙ¼ø¼øïïé@¿óéJîåïðÅÙïåÂ¯ïçÔ¿éKÏâÄ÷ïéÂà×êöÇÔäïãéE³¤ÃÅãÅÉÁãÆê\±Õ¿ªãÊãÈÈòÏÐÏÐ¼äãÉÕ¢ÄÖºÒ¹Ø¸óºÏ·§¹ëÃöãÍãÏãÌÔÄÔÄãÑÑËÑÖãÕãÔãÐãÓãÖ°åãÇÀ«ã×À»ê^ãÙê`ê]ãØãÚ´³¿ú¹ØãÛê_²û±ÙêaãË¿ÓÖ·ÚêÉÂÉýÕóÒõ³ÂÂ½ÑôÒõµÌÚí¶Ó½×ÔÉÎë¼ÊÁÚËæÏÕÒþÂ¤Á¥Á¥Ö»öÁËäË«³ûÔÓ¼¦ÀëÄÑÔÆµçÕ´ÁéÁïÎíö«ö¨ö°…¦Áé…¥ö¦¾²ÃæëïØÌÈÍØ»Ø»¹®Çï÷³çÖ÷²Ç§÷µÎ¤ÈÍí‚º«è¸èº÷¹è¹ÍàÔÏÏìÒ³¶¥ÇêÏîË³ñüÐëçïËÌñýñþÔ¤Íç°ä¶ÙÆÄÁìò¢ò¡ÒÃò¤¸©Í·ïH¼ÕïFïIò¥¾±ÍÇÆµÍÇ¿ÅÌâ¶îò¦ÑÕïJò§ÑÕÔ¸òªµßÀàò©ò«¹Ë²üò¬ÏÔò­Â­ò¨È§·çïsì©ìªÌ¨¹Îì«ïtÑïïuì¬ïvÆ®Æ®ì­ì­ì­·É¼¢ð—ð˜â½â¿âÀâÁ·¹ÒûâÂËÇ±¥ÊÎð™½Èðš±ýâÃÑø¶üð›ðœâÄÄÙ¶öðžðÓàëÈâÆðŸ½¤ÏÚ¹Ýô×â¼Î¹ð âÇñAâ¾À¡ÁóâÈñ@âÉÂøâÊâËâÌÀ¡âÍ¼¢ÈÄ÷Ï÷ÐâÉ²öâÎÂíÔ¦·ëÍÔ³ÛÑ±óR²µ×¤æå¾Ôæà¼ÝæææâÊ»ÍÕæáÂîæéº§²µóSÂæóV¿¥³ÒóUæíóWæìÆïæëÑéæðÆ­Æ­××óYå¹æïæòÌÚæãÉ§æóÂâÝëæñæîæôæõÇýæèóXæçæö½¾ÑéÂâ¾ªæäÖèÂ¿æøæ÷óZæêóTöá÷ÃÔàÌå÷Æ÷Å÷Þ·¢ðßËÉºúÐë÷Þ¶·¶·ÄÖºåãÒ¶·ãÎÓô÷Ë÷ÊÓã÷÷‚÷ƒÂ³öÐöÏ÷…öÑöÒ÷ˆ÷†öç÷‰öÓöØ±«öÖ÷‡öÚ÷öÜ÷‹öÛöÞöÙÏÊ÷Š÷÷–÷öçöáöéÀðöè÷‘öö÷’öëößöôöñöîöòöïöð¾¨öìöíöóöõ÷“÷–öêöý÷™÷Œ÷˜öøöüöúöú÷”öùöûÈú÷—÷œ÷›öþöå÷¤÷£÷¥÷¢÷—öã÷¡÷¦÷ªöæöä÷“÷©÷ž÷§÷š÷«÷¨±î÷®÷­÷­÷¬ÁÛöàö÷ö×÷Žø@÷ ÷¯÷•öÝ÷ŸöùöÔöâÄñÙìð¯û\·ïÃùð°ð²ð±Ñ»û_ÍÒÔ§ð¶û^ð·ð³ÑìÑ¼û`ð¹ð»ûaºè¸ëð¼ûbûc¾éðÁð¾ðÃ¶ì¶ìðÀðÄðÆÅôûeðÇÈµÑ»ûgûdð´ðÈûfðÅûjðÉðÊûiûdðÌðÍû]Ýºûlº×ûmûn÷½ðÏðËðËûoðÎ¼¦ûkðÑûpÅ¸ðºðÒð¸ðÔûhðÓÑàðÕðÂðÂðÖÓ¥ðØðÂûrð×ÝºûsðµûtðÐðÙð¿ð½Â±ÏÌõº¼îÑÎáóÀöÂóôïÃæÇúÃæÃ´»ÆÙäµãµ³÷õÃ¹üd÷òö¼ö½ü…÷¡±îö¾¶¬÷ú÷þÆëÕ«êåì´³Ýö³ý†ý‡öµö·ö´ö¶Áä³öö¸Äööºö¹È£ëñö»ÁúÅÓ¹¨íè¹ê¼³»²Â¾ëÐ¾»É²¿ï´çÄÅ³ßÉÊÄãìéÌöÑÒÀìØÆÓ¸ß±îáØì²æÇ¤µÖÞÕ²ÁéæÒ·¾Öß¦µ§êØÁËÊõÛØèÙÎãÍèÀï±ñÏâî´ðòÂéÂéãÄØºÃÐÁËììÖ»óÌóë’I¸¿²ÅíüÜÐ¹¶éÂÊíÞÁ°Ú½î×ÅÅöÇ¹Ï³îã¶òÉÂ°¹å¹Ø£";
    private static final String GBK_S2T_S_MAP = "°¨°ª°­°®°À°Â°Ó°Õ°Ú°Ü°ä°ì°í°ï°ó°÷°ù°þ±¥±¦±¨±«±²±´±µ±·±¸±¹±Á±Ê±Ï±Ð±Ò±Õ±ß±à±á±ä±ç±è±ê±î±ð±ñ±ô±õ±ö±÷±ý²¢²¦²§²¬²µ²·²¹²Æ²Î²Ï²Ð²Ñ²Ò²Ó²Ô²Õ²Ö²×²Þ²à²á²â²ã²ï²ó²ô²õ²ö²÷²ø²ù²ú²û²ü³¡³¢³¤³¥³¦³§³©³®³µ³¹³¾³Â³Ä³Å³Æ³Í³Ï³Ò³Õ³Ù³Û³Ü³Ý³ã³å³æ³è³ë³ì³ï³ñ³ó³÷³ø³ú³û´¡´¢´¥´¦´«´¯´³´´´¸´¿´Â´Ç´Ê´Í´Ï´Ð´Ñ´Ó´Ô´Õ´Ú´Ü´í´ï´ø´ûµ£µ¥µ¦µ§µ¨µ¬µ®µ¯µ±µ²µ³µ´µµµ·µºµ»µ¼µÁµÆµËµÐµÓµÝµÞµßµãµæµçµíµöµ÷µýµþ¶¤¶¥¶§¶©¶ª¶«¶¯¶°¶³¶·¶¿¶À¶Á¶Ä¶Æ¶Í¶Ï¶Ð¶Ò¶Ó¶Ô¶Ö¶Ù¶Û¶á¶é¶ì¶î¶ï¶ñ¶ö¶ù¶û¶ü·¡·¢·£·§·©·¯·°·³·¶···¹·Ã·Ä·É·Ì·Ï·Ñ·×·Ø·Ü·ß·à·á·ã·æ·ç·è·ë·ì·í·ï·ô·ø¸§¸¨¸³¸´¸º¸¼¸¾¸¿¸Ã¸Æ¸Ç¸É¸Ï¸Ñ¸Ó¸Ô¸Õ¸Ö¸Ù¸Ú¸ä¸é¸ë¸ó¸õ¸ö¸ø¹¨¹¬¹®¹±¹³¹µ¹¹¹º¹»¹Æ¹È¹Ë¹Ð¹Ò¹Ø¹Û¹Ý¹ß¹á¹ã¹æ¹è¹é¹ê¹ë¹ì¹î¹ñ¹ó¹ô¹õ¹ö¹ø¹ú¹ýº§º«ºººÅºÒº×ºØºáºäºèºìºóºø»¤»¦»§»©»ª»­»®»°»³»µ»¶»·»¹»º»»»½»¾»À»Á»Æ»Ñ»Ó»Ô»Ù»ß»à»á»â»ã»ä»å»æ»ç»ë»ï»ñ»õ»ö»÷»ú»ý¼¢¼£¼¥¼¦¼¨¼©¼«¼­¼¶¼·¼¸¼»¼Á¼Ã¼Æ¼Ç¼Ê¼Ì¼Í¼Ð¼Ô¼Õ¼Ö¼Ø¼Û¼Ý¼ß¼à¼á¼ã¼ä¼è¼ê¼ë¼ì¼î¼ï¼ð¼ñ¼ò¼ó¼õ¼ö¼÷¼ø¼ù¼ú¼û¼ü½¢½£½¤½¥½¦½§½ª½«½¬½¯½°½±½²½´½º½½½¾½¿½Á½Â½Ã½Ä½Å½È½É½Ê½Î½Ï½×½Ú½Ü½à½á½ë½ì½ô½õ½ö½÷½ø½ú½ý¾¡¾¢¾£¾¥¾¨¾ª¾­¾±¾²¾µ¾¶¾·¾º¾»¾À¾Ç¾É¾Ô¾Ù¾Ý¾â¾å¾ç¾é¾î¾õ¾ö¾÷¾ø¾û¾ü¿¥¿ª¿­¿Å¿Ç¿Î¿Ñ¿Ò¿Ù¿â¿ã¿ä¿é¿ë¿í¿ó¿õ¿ö¿÷¿ù¿úÀ¡À£À©À«À¯À°À³À´ÀµÀ¶À¸À¹ÀºÀ»À¼À½À¾À¿ÀÀÀÁÀÂÀÃÀÄÀÌÀÍÀÔÀÖÀØÀÝÀàÀáÀéÀëÀïÀðÀñÀöÀ÷ÀøÀùÀúÁ¤Á¥Á©ÁªÁ«Á¬Á­Á¯Á°Á±Á²Á³Á´ÁµÁ¶Á·Á¸Á¹Á½Á¾ÁÂÁÆÁÉÁÍÁÔÁÙÁÚÁÛÁÝÁÞÁäÁåÁèÁéÁëÁìÁóÁõÁúÁûÁüÁýÂ¢Â£Â¤Â¥Â¦Â§Â¨Â«Â¬Â­Â®Â¯Â°Â±Â²Â³Â¸Â»Â¼Â½Â¿ÂÀÂÁÂÂÂÅÂÆÂÇÂËÂÌÂÍÂÎÂÏÂÐÂÒÂÕÂÖÂ×ÂØÂÙÂÚÂÛÂÜÂÞÂßÂàÂáÂâÂæÂçÂèÂêÂëÂìÂíÂîÂðÂòÂóÂôÂõÂöÂ÷ÂøÂùÂúÃ¡Ã¨ÃªÃ­Ã³Ã´Ã¹Ã»Ã¾ÃÅÃÆÃÇÃÌÃÎÃÕÃÖÃÙÃÝÃàÃåÃíÃðÃõÃöÃùÃúÃýÄ±Ä¶ÄÆÄÉÄÑÄÓÄÔÄÕÄÖÄÙÄÚÄâÄåÄìÄíÄðÄñÄôÄöÄ÷ÄøÄûÄüÄþÅ¡Å¢Å¥Å¦Å§Å¨Å©Å±ÅµÅ·Å¸Å¹Å»Å½ÅÌÅÓÅ×ÅâÅçÅôÆ­Æ®ÆµÆ¶Æ»Æ¾ÆÀÆÃÆÄÆËÆÌÆÍÆÓÆ×ÆÜÆàÆêÆëÆïÆñÆôÆøÆúÆýÇ£Ç¤Ç¥Ç¦Ç¨Ç©Ç«Ç®Ç¯Ç±Ç³Ç´ÇµÇ¹ÇºÇ½Ç¾Ç¿ÇÀÇÂÇÅÇÇÇÈÇÌÇÏÇÔÇÕÇ×ÇÞÇáÇâÇãÇêÇëÇìÇíÇîÇ÷ÇøÇûÇýÈ£È§È¨È°È´ÈµÈ·ÈÃÈÄÈÅÈÆÈÈÈÍÈÏÈÒÈÙÈÞÈíÈñÈòÈóÈ÷ÈøÈúÈüÈþÉ¡É¥É§É¨É¬É±É´É¸É¹É¾ÉÁÉÂÉÄÉÉÉËÉÍÉÕÉÜÉÞÉãÉåÉèÉðÉóÉôÉöÉøÉùÉþÊ¤Ê¥Ê¦Ê¨ÊªÊ«Ê¬Ê±Ê´ÊµÊ¶Ê»ÊÆÊÊÊÍÊÎÊÓÊÔÊÙÊÞÊàÊäÊéÊêÊôÊõÊ÷ÊúÊýË§Ë«Ë­Ë°Ë³ËµË¶Ë¸Ë¿ËÇËÊËËËÌËÏËÐËÓËÕËßËàËäËæËçËêËïËðËñËõËöËøÌ¡Ì¢Ì¬Ì¯Ì°Ì±Ì²Ì³Ì·Ì¸Ì¾ÌÀÌÌÌÎÌÐÌÖÌÚÌÜÌàÌâÌåÌëÌõÌùÌúÌüÌýÌþÍ­Í³Í·ÍºÍ¼Í¿ÍÅÍÇÍÉÍÑÍÒÍÔÍÕÍÖÍÝÍàÍäÍåÍçÍòÍøÎ¤Î¥Î§ÎªÎ«Î¬Î­Î°Î±Î³Î½ÎÀÎÂÎÅÎÆÎÈÎÊÎÍÎÎÎÏÎÐÎÑÎÔÎØÎÙÎÚÎÛÎÜÎÞÎßÎâÎëÎíÎñÎóÎýÎþÏ®Ï°Ï³Ï·Ï¸ÏºÏ½Ï¿ÏÀÏÁÏÃÏÅÏÇÏÊÏËÏÌÏÍÏÎÏÐÏÔÏÕÏÖÏ×ÏØÏÚÏÛÏÜÏßÏáÏâÏçÏêÏìÏîÏôÏùÏúÏþÐ¥Ð«Ð­Ð®Ð¯Ð²Ð³Ð´ÐºÐ»Ð¿ÐÆÐËÐÚÐâÐåÐéÐêÐëÐíÐðÐ÷ÐøÐùÐüÑ¡Ñ¢Ñ¤Ñ§Ñ«Ñ¯Ñ°Ñ±ÑµÑ¶Ñ·Ñ¹Ñ»Ñ¼ÑÆÑÇÑÈÑËÑÌÑÎÑÏÑÕÑÖÑÞÑáÑâÑåÑèÑéÑìÑîÑïÑñÑôÑ÷ÑøÑùÑþÒ¡Ò¢Ò£Ò¤Ò¥Ò©Ò¯Ò³ÒµÒ¶Ò½Ò¿ÒÃÒÅÒÇÒÏÒÕÒÚÒäÒåÒèÒéÒêÒëÒìÒïÒñÒõÒøÒûÒþÓ£Ó¤Ó¥Ó¦Ó§Ó¨Ó©ÓªÓ«Ó¬Ó®Ó±Ó´ÓµÓ¶Ó¸Ó»Ó½Ó¿ÓÅÓÇÓÊÓËÓÌÓÎÓÕÓÚÓßÓãÓæÓéÓëÓìÓïÓôÓõÓùÓüÓþÔ¤Ô¦Ô§Ô¨Ô¯Ô°Ô±Ô²ÔµÔ¶Ô¸Ô¼Ô¾Ô¿ÔÀÔÁÔÃÔÄÔÆÔÇÔÈÔÉÔËÔÌÔÍÔÎÔÏÔÓÔÖÔØÔÜÔÝÔÞÔßÔàÔäÔæÔðÔñÔòÔóÔôÔùÔúÔýÔþÕ¡Õ¢Õ¤Õ©Õ«Õ®Õ±ÕµÕ¶Õ·Õ¸Õ»Õ½ÕÀÕÅÕÇÕÊÕËÕÍÕÔÕÝÕÞÕàÕâÕêÕëÕìÕïÕòÕóÕõÕöÕøÕùÖ¡Ö£Ö¤Ö¯Ö°Ö´Ö½Ö¿ÖÀÖÄÖÊÖÍÖÓÖÕÖÖÖ×ÖÚÖßÖáÖåÖçÖèÖíÖîÖïÖòÖõÖöÖüÖýÖþ×¤×¨×©×ª×¬×®×¯×°×±×³×´×¶×¸×¹×º×»×Å×Ç×È×Ê×Õ×Ù×Û×Ü×Ý×Þ×ç×é×êØ¨Ø»ØÂØÄØÇØÉØËØÌØÍØÐØÑØÓØÙØÛØÜØñØöØ÷ØùÙ­Ù¯Ù±Ù²Ù³Ù¶ÙÇÙÌÙÍÙÎÙÏÙÐÙÝÙáÙäÙæÙìÙðÙòÙôÙõÙ÷Ú£Ú¥Ú¦Ú§Ú¨Ú©ÚªÚ«Ú¬Ú­Ú®Ú¯Ú°Ú±Ú²Ú³Ú´ÚµÚ¶Ú·Ú¸Ú¹ÚºÚ»Ú¼Ú½Ú¾Ú¿ÚÀÚÁÚÂÚÃÚÄÚÅÚÆÚÇÚÈÚÉÚÊÚËÚÌÚÍÚÎÚÏÚÐÚÑÚÒÚÓÚÔÚÕÚÖÚ×ÚØÚÙÚÚÚÛÚÜÚÝÚÞÚßÚáÚêÚíÚ÷ÚùÚþÛ£Û¦Û§Û©ÛªÛ»Û¼Û½ÛÂÛÊÛÏÛÑÛÛÛÞÛàÛâÛäÛëÛîÛðÛñÛõÛöÛ÷ÛûÛþÜ«Ü³Ü¼ÜÂÜÈÜÉÜÊÜÑÜ×ÜÜÜàÜãÜäÜéÜêÜñÜöÜùÜýÜþÝ¡Ý£Ý¤Ý¥Ý¦Ý§ÝªÝ«Ý¯Ý°Ý²ÝµÝºÝ»ÝÓÝÛÝÞÝäÝëÝñÝöÝ÷ÝüÝþÞ­Þ´ÞºÞ»ÞÆÞÏÞÑÞÒÞØÞâÞèÞêÞìÞóÞüß¢ß£ß¥ß´ß¼ß½ß¿ßÂßÃßÄßÇßÌßÐßÔßÕßØßÙßÜßßßàßâßåßæßéßëßïßðßõßùßüà¶à·à¿àÀàÈàÎàÓàààèàëàðàøàüàýàþá¥á«á­á®á°á´á»á½á¿áÀáÁáÉáÎáÐáÕáÛáâáëáîáïáóáöáøáýâ¤â¨â¬â¼â½â¾â¿âÀâÁâÂâÃâÄâÅâÆâÇâÈâÉâÊâËâÌâÍâÎâÐâÙâÞâãâäâæâéâêâëâøâúâûâüâýã¢ã¥ã«ã³ã´ãÀãÁãÅãÆãÇãÈãÉãÊãËãÌãÍãÎãÏãÐãÑãÒãÓãÔãÕãÖã×ãØãÙãÚãÛãÜãããíãñãòãøãþä¤ä¥ä«ä¯ä°ä±äµä¶äÂäÅäÉäËäÓäÙäÜäÞäääëäìäíäòäóäþå°å¹åÇåÉåÎåðåòåüåýæ£æ©æ«æ¬æ®æ´æµæ¿æÁæÈæÉæÍæÖæàæáæâæãæäæåæææçæèæéæêæëæìæíæîæïæðæñæòæóæôæõæöæ÷æøæùæúæûæüæýæþç¡ç¢ç£ç¤ç¥ç¦ç§ç¨ç©çªç«ç¬ç­ç®ç¯ç°ç±ç²ç³ç´çµç¶ç·ç¸ç¹çºç»ç¼ç½ç¾ç¿çÀçÁçÂçÃçÄçÅçÆçÇçÈçÉçÊçËçÌçÍçÎçÏçÐçÑçÒçÓçÔçÕçÖç×çØçÙçÚçÛçáçâçåçççëçïçôçõçöè¨è¬è¯è¶è¸è¹èºè¿èÀèÅèÇèÈèÉèÎèÐèÓèÙèÝèßèâèãèåèçèëèíèïèðèùèüèýé¡é¤é­é´éµé·éÄéÆéÉéÍéÖéÚéÜéÝéâéäéæéçéééëéíéîéïéðéñéòéóéôéõéöé÷éøéùéúéûéüéýéþê¡ê¢ê£ê¤ê¥ê§ê¨ê¯ê±ê¼êÊêÍêÓêÚêÛêÜêÝêÞêßêàêáêâêãêäêåêæêçêèêéêêêëêìêíêîêïêñêóë§ëªë²ëµë¹ëÉëÊëËëÍëÖëÚëáëçëïëðëñë÷ì£ì©ìªì«ì¬ì­ì®ì±ì´ìµì¾ì¿ìÀìÁìÇìËìÎìÑìÕìÖìâìòìõìøí¡í¨íªí¯í°í³í´í¶í¸íºí¿íÂíÃíÌíÍíÓí×íÛíÞíèíîíöíùíúî´î¼î¿îÅîÆîÇîÈîÉîÊîËîÌîÍîÎîÏîÐîÑîÓîÔîÕîÖî×îØîÙîÚîÛîÜîÝîÞîßîàîáîâîãîäîåîæîçîèîéîêîëîìîíîîîïîðîñîòîóîôîõîöî÷îøîùîúîûîüîýîþï¡ï¢ï£ï¤ï¥ï¦ï§ï¨ï©ïªï«ï¬ï­ï®ï¯ï°ï±ï²ï³ï´ïµï¶ï·ï¸ï¹ïºï»ï¼ï½ï¾ï¿ïÀïÁïÂïÃïÄïÅïÆïÇïÈïÉïÊïËïÌïÍïÎïÏïÐïÑïÒïÓïÔïÕïÖï×ïØïÙïÚïÛïÜïÝïÞïßïàïáïâïãïäïåïæïçïèïéïêïëïìïíïîïïïðïñïùð£ð¯ð°ð±ð²ð³ð´ðµð¶ð·ð¸ð¹ðºð»ð¼ð½ð¾ð¿ðÀðÁðÂðÃðÄðÅðÆðÇðÈðÉðÊðËðÌðÍðÎðÏðÐðÑðÒðÓðÔðÕðÖð×ðØðÙðÜðÝðßðâðåðéðìðïð÷ðùðüñ¨ñ«ñ®ñ²ñ³ñ¼ñÀñÉñÍñÏñÐñÚñÜñßñäñìñïñ÷ñùñüñýñþò¡ò¢ò£ò¤ò¥ò¦ò§ò¨ò©òªò«ò¬ò­ò°ò±ò²ò¹òºòÃòÉòÌòÍòÏòÓòåòîò÷òýó¿óÆóÈóÖóÙóÝóåóæóêóìóïóñóýô¥ô¯ôµôÁôÇôÌôÏôÐôÖô×ôêôïôõõ£õ¦õ§õºõ»õÄõÅõÈõÎõÏõÑõÒõÙõÜõæõçõéõïõòõüö£ö¦ö¨ö«ö°ö³ö´öµö¶ö·ö¸ö¹öºö»ö¼ö½ö¾öÁöÅöÇöÉöÏöÐöÑöÒöÓöÔöÕöÖö×öØöÙöÚöÛöÜöÝöÞößöàöáöâöãöäöåöæöçöèöéöêöëöìöíöîöïöðöñöòöóöôöõööö÷öøöùöúöûöüöýöþ÷¡÷¢÷£÷¤÷¥÷¦÷§÷¨÷©÷ª÷«÷¬÷­÷®÷¯÷²÷³÷µ÷¹÷½÷Ã÷Å÷Æ÷Ê÷Ë÷Ï÷Ð÷Þ÷ò÷õ÷ú÷þ„i„Ö…¥…¦†y†|†ª†®ˆ™ˆ›‰G‰×‹OŒÁNiÂŽö¦‘\’¥’¦•D•o—ƒ—…˜–›h›m›¸›º›»›éœ¾œùªA«_«`«o«š¬Q¯}±€³n³}³~µt¶Œ¹Y¹‚À€ÀÀ‚ÀƒÀ„À…À†À‡ÀˆÀ‰ÀŠÁ™ÁšÄNÅHÇQÈ[Î…Ï]Ð„ÑBÑTÒ[Ó_Ó`Ô€×š×›×œ××ž× ØkÚOÚPÚQÚRÚSÚWÛQÞaÞbÞcÞdáNâ è•è–è—è™èšè›èœèèžèŸè é@éAéBéCéDéEéFéGéHéIéJéKê\ê]ê^ê_ê`êaí‚ïFïGïHïIïJïKïrïsïtïuïvð—ð˜ð™ðšð›ðœððžðŸð ñ@ñAóRóSóTóUóVóWóXóYóZ÷÷‚÷ƒ÷…÷†÷‡÷ˆ÷‰÷Š÷‹÷Œ÷÷Ž÷÷÷‘÷’÷“÷”÷•÷–÷—÷˜÷™÷š÷›÷œ÷ž÷Ÿ÷ ø@û\û]û^û_û`ûaûbûcûdûeûfûgûhûiûjûkûlûmûnûoûpûrûsûtüdü…ý†ý‡’I°¹¸Ë¹¶ÀÅÀêÃÐÄÅÉ²ÉÊÌ¨Ð×ÑÒÖ¢×¼ØºÜ¿ÜÐÞÁß¦ß±íüîÒ÷á";
    private static final String GBK_S2T_T_MAP = "°}Ì@µKÛÒ\ŠW‰ÎÁT”[”¡îCÞk½OŽÍ½‰æ^Ör„ƒï–ŒšˆóõUÝ…Øä^ªN‚ä‘v¿‡¹P®…”ÀŽÅé]ß…¾ŽÙH×ƒÞqÞp˜Ë÷M„e°TžlžIÙe”PïžK“ÜÀãKñgÊNÑaØ”…¢ÐQšˆ‘M‘K NÉnÅ“‚}œæŽú‚ÈƒÔœyŒÓÔŒ”v“½Ïsð’×‹ÀpçP®aêUîˆö‡LéLƒ”ÄcS•³ânÜ‡Ø‰mêÒr“Î·Q‘ÍÕ\òG°VßtñYuýXŸë›_ÏxŒ™® ÜP»I¾Iáh™»NäzërµAƒ¦Ó|ÌŽ‚÷¯êJ„“åN¼ƒ¾bÞoÔ~ÙnÂ”Ê[‡èÄ…²œÜf¸Zåeß_Ž§ÙJ“ú†Îà“ÛÄ‘‘„ÕQ—®”“õühÊŽ™n“vu¶\Œ§±IŸôà‡”³œìßf¾†îüc‰|ëŠÕážÕ{Õ™¯Bá”í”åVÓ†G–|„Ó—ƒöôY Ùªš×xÙ€åƒå‘”à¾„ƒ¶ê Œ¦‡îDâgŠZ‰™ùZî~ÓžºðIƒº –ðDÙE°lÁPéy¬mµ\âCŸ©¹ ØœïˆÔL¼ïwÕuUÙM¼Š‰žŠ^‘¼SØS—÷ähïL¯‚ñT¿pÖSøPÄwÝ—“áÝoÙxÑ}Ø“Ó‡‹D¿`Ô“â}ÉwŽÖÚs¶’ÚMŒù„‚ä“¾Væ€”Røéwãt‚€½oýŒmì–Ø•ã^œÏ˜‹Ù‰òÐM·Yî™„Ž’ìêPÓ^ð^‘TØžVÒŽÎùšwý”é|Ü‰ÔŽ™™ÙF„£ÝLå‡øß^ñ”ínhÌ–éuúQÙR™MÞZø™¼táá‰Ø×oœû‘ô‡WÈA®‹„Ô’‘Ñ‰Äšg­hß€¾“Q†¾¯ˆŸ¨œoüSÖe“]Ýxš§ÙV·x•þ Z…RÖMÕdÀLÈœ†â·«@Ø›µœ“ô™C·eð‡ÛE×Iëu¿ƒ¾ƒ˜OÝ‹¼‰”DŽ×ËE„©úÓ‹Ó›ëHÀ^¼oŠAÇvîaÙZâ›ƒrñ{šž±OˆÔ¹{égÆD¾}ÀO™z‰Aû|’þ“ìº†ƒ€œpË]™‘èbÛ`ÙvÒŠæIÅž„¦ðTužR¾ËKŒ¢{ÊY˜ªª„ÖváuÄz²òœ‹É”‡ãq³CƒeÄ_ïœÀU½gÞIÝ^ëA¹‚Ü½YÕ]ŒÃ¾oå\ƒHÖ”ßM•x a±M„ÅÇGÇoöLó@½›îiìoçR½¯d¸‚œQ¼mŽýÅfñxÅe“þä‘Ö„¡ùN½ÓX›QÔE½^âxÜŠòEé_„Pîwš¤Õn‰¨‘©“¸ŽìÑÕF‰Kƒ~Œ’µV•ç›rÌŽh¸Qð¢”UéŸÏžÅDÈRíÙ‡Ë{™Ú”r»@ê@Ìmž‘×Ž”ˆÓ[‘ÐÀ| €žE“Æ„Ú³˜·èD‰¾îœI»hëxÑYõŽ¶Yû…–„îµ[•Ñžrë`‚zÂ“ÉßBç ‘ziºŸ”¿Ä˜æœ‘ÙŸ’¾š¼Z›öƒÉÝvÕ¯Ÿß|ç‚«CÅRà÷[„CÙUýgâœRì`ŽXîIðs„¢ýˆÃ@‡µ»\‰Å”në]˜ÇŠä“§ºtÌJ±RïB] t“ïûuÌ”ô”ÙTµ“ä›ê‘óH…ÎäX‚HŒÒ¿|‘]žV¾GŽn”Œ\ž´y’àÝ†‚öœS¾]Õ“Ì}Á_ß‰èŒ»jò…ñ˜½j‹Œ¬”´aÎ›ñRÁR†áÙIûœÙuß~Ã}²mðzÐUMÖ™Øˆå^ãTÙQüNüq›]æVéTž‚ƒåi‰ôÖi›Ò’ƒç¾d¾’Rœç‘‘é}øQã‘Ö‡Ö\®€âc¼{ëy“ÏÄXÀô[ðHƒÈ”MÄ”f“Óá„øBÂ™ýmè‡æ‡™ŽªŸŒŽ”Qôâo¼~Ä“âÞr¯‘ÖZšWútšª‡Ia±Pý‹’Ùr‡Šùiò_ïhîlØšÌO‘{ÔuŠîH“ääƒW˜ã×V—«œDÄšýRòTØM†¢šâ—‰Ó™ ¿’LâTãUßwºžÖtåXãQ“œ\×l‰q˜Œ†Ü ËNŠ“Œæ@˜ò†ÌƒSÂN¸[¸`šJÓHŒ‹ÝpšäƒAí•Õˆ‘c­‚¸FÚ……^Ü|òŒýxïE™à„ñ…sùo´_×Œðˆ”_À@ŸáígÕJ¼x˜s½qÜ›äJéc™ž¢Ë_öwÙÈý‚ã†Êò}’ß­š¢¼†ºY•ñ„héWê„Ù ¿˜‚ûÙpŸý½BÙd”z‘ØÔO¼Œ‹ðÄIBÂ•ÀK„ÙÂ}ŽŸª{ñÔŠŒÆ•rÎgŒ×Rñ‚„ÝßmáŒï—Ò•Ô‡‰Û«F˜ÐÝ”•øÚHŒÙÐg˜äØQ”µŽ›ëpÕl¶í˜Õf´T q½zï•Â–‘ZížÔAÕb”\ÌKÔVÃCëmëS½—šqŒO“p¹S¿s¬æi«H“é‘B”‚Ø°cž©‰¯×TÕ„šUœ« Cý½dÓ‘òvÖ`äRî}ówŒÏ—lÙNèFdÂ ŸNã~½yî^¶dˆD‰TˆFîjÍ‘Ã“ørñWñ„™E¸DÒmž³îBÈf¾Wífß`‡úžéžH¾SÈ”‚¥‚Î¾•Ö^ÐlœØÂ„¼y·€†–®Y“ëÎœu¸CÅP†èæužõ›@Õ_ŸoÊ…Ç‰]ìF„ÕÕ`åa ÞÒuÁ•ãŠ‘ò¼šÎrÝ {‚bªMB‡˜åvõrÀwûyÙtã•éeï@ëU¬F«I¿hðWÁw‘—¾€Žûè‚àlÔ”í‘í—Ê’‡ÌäN•Ô‡[Ï…f’¶”yÃ{ÖCŒ‘žaÖxä\á…Åd›°çnÀCÌ“‡uíšÔS”¢¾wÀmÜŽ‘Òßx°_½kŒW„ìÔƒŒ¤ñZÓ–Óßd‰ºøfø††¡†Ó éŽŸŸû}‡ÀîéØW…’³Ž©ÖVòžø„—î“P¯ƒê–°WðB˜Ó¬Ž“uˆòßb¸GÖ{ËŽ ”í“˜IÈ~átãžîUßzƒxÏË‡ƒ|‘›ÁxÔ„×hÕx×g®À[ÊaêŽãyï‹ë[™Ñ‹ëú—‘ªÀt¬“Îž IŸÉÏ‰ÚA·f†Ñ“í‚ò°bÛxÔœ¥ƒž‘nà]â™ªqß[ÕTì¶Ý›ô~OŠÊÅcŽZÕZôd»n¶Rªz×uîAñSøxœYÞ@ˆ@†TˆA¾‰ßhîŠ¼sÜSè€Ž[»›‚é†ë…ày„òëEß\ÌNáj•žíësžÄÝd”€•ºÙÚEóvè——ØŸ“ñ„tÉÙ\Ù›¼™„žÜˆåŽél–ÅÔpýS‚ùšÖ±K”ØÝšä—£‘ð¾`ˆqŽ¤Ù~Ã›ÚwÏUÞHæNß@Ø‘á˜‚ÉÔ\æ‚ê‡’ê± ªb ŽŽ¬à×C¿—ÂšˆÌ¼ˆ“´”SŽÃÙ|œþçŠ½K·NÄ[±ŠÖaÝS°™•ƒóEØiÖTÕD T²š‡ÚÙAèTºBñvŒ£´uÞDÙ˜¶ÇfÑbŠy‰Ñ îåFÙ˜‰‹¾YÕÖøáÆÙYnÛ™¾C¿‚¿vàuÔ{½MèƒìŠÁd†Ý…‡…˜PìvÚI…Q…TÙ‘„q„¥„’‚ø‚t‚áÐƒŠƒzƒ‰ƒ°ƒ«‚Rƒf‚ôƒEƒ¯ƒ†ƒ®ƒL¼eüZ‡ÏøDƒ¼Ð–ÒCÅL·A‰VÓ…Ó“ÓÓ˜ÖŽÔnÔGÔbÔXÔgÔtÔxÔrÕEÕCÔŸÔ‘ÔœÔ–ÔÔÕŠÕŸÔ‚ÕVÕaÕNÕOÕŒÕŽÕ†Õ˜Õ”Õ~ÕrÖRÖGÖoÖ]Ö@ÖIÖXÖOÖBÖJÕ›Öƒ×•ÖqÕžÖkÖ†×v×P×S×H×—×d×Ž„ê€êŸà—àwà’àPà”àSàiáBÆcŠJ„êÃÍšëŽ€ˆ×‰¿‰ÈÚæ‰Å‰Àˆº‰Nˆsˆß‰Pˆå‰_ˆ‰|Ô­ÆHËGËžÇ{ÈOÉÆrÌd™”Ê\‰LŸ¦ÊÉœÊwËCËj ÎœîÊnË|Ê{ÉpÈ‡È’ÉPÈnÉ‰ÉWËWÊ~úLÉ”¿MÊrÊ‰ÊVò‡ÌyævÊšÌ`ÌAÌIË’Ì\éÂŠYŒÀ’Ð“»“×““¥“«“å”d”t”X”]”x‡\‡`‡Ò‡³†h†J†w†U‡“‡z‡j‡}‡^†ô‡‚ßÉ‡ˆ‡ß¸‡‡O‡Z†î†r‡K‡Êºô‡D‡¿‡ËºÇ‡†Þ\‡Â‡£ÅüÖo‡÷Ž®ŽÎŽ¾Ž½ÒLçsŽS¹–ŽFþ˜÷ˆŽV£â¼¹ŽpÆÈ®«Eªwûƒªœªsª«M«JÎoðhï‚ðqïƒï„ï†ïðAðGðNðQðlðtðxð}ð~ð€ð‚ð–TÙs[‘Ô‘“‘Y÷í‘«‘Q‘ÃÅðÁ‘aÜ‘C‘|âð‘¬éVéZéébéhé`êYé‚é€ôbéé“é‹ô]é”é’é‘é˜é êHêDêIêRãÝž–œ¿ž{žožT›Ü›ÑœÒžgG¡œZ¬ž^ÆžcœOsž¹ž—ž]§žužtžEž‡žzž|ž®òqßƒÞŸßŠŒÕ†‹³‹ž‹‚Š™‹I‹ÆŒD‹z‹¹‹È‹‹‹Ü‹å‹Ô‹ßñzñ†ñ€ò|óAñwñ~ò”ò‘ñ‰óPòUòSòKò‰òsò\òˆòtò~òŠò‹ò–óKóJôé¼u¼q¼v¼wÀk¼‹¼„¼‚½C½X¼›¿U½E½I½H½f½W½{½Ž½‹½¾c¾_¾p¾y¾i¾E¾R¾^¾J¾U¾l¾~¾|¾Ÿ¾˜ÀD¾Œ¾œ¾—¿P¾‡¿N¿b¿d¿c¿r¿O¿VÀ_¿~¿z¿w¿Š¿‰Ài¿¿•í\À`ÀRÀQÀyÃ´­^¬|«k­‡¬zíœ­t¬q­I­a­‹­v­‘ítíyíw˜q™À—g—–˜º—n™±™É™¾—d™µ™f—¿˜ï˜E˜˜å™u™è—¨™ô™³˜ ™å˜¡™ì™Â™°™Î™‰™½™{™Á™©™´º™™_š{š‘šŒššš—š›ÜÜ—ÝMÝVÞ_ÝTÝWÝFÜ Þ]ÝUÝYÝeÝbÝ`ÝmÝ‚ÝyÝzÝwÝÞAÞO‘â‘ê‘ì®T•Ò•Ï•Ÿ•áÙSÙBÙLÙOÙ—ÙDÙWÚBÙcÙlÙgýVÙyÙŽÒ—ÓJÒ Ó]ÓDÓMÓPÓUŠ ÓšÐšÚšåšè ©Äd–VÄLÅFÃ„Ä’ÄTáZìtÄeý|ÄœšeïRïSïZï`ïjïjÝžýW”ÌŸ¬Ÿ˜ŸõŸÍŸî FºýŸÍËÁï c¶[µ¶U‘»âœ¡‘¿‘ßßeÍ´‰´X³Œ´^µZµa³ˆ´“´ƒ´~ïàLý²g±{²A²€ÙÜÁ`Ábááá•á“á‘âQâAáŸå{âOâSâbââkâjâ[â‚â^â€âZâ•ã`â’âŽãOâ˜â“ãXãfãgâšèpâ‹ãCãBãGâ‰â”èIäDã™ãsäBä…äeçtäyèKäHãŸæzãäbäAçfãŒãxæ|ã“åPäCã|ç|ä@ãœç„ånäˆçHä{ä‡ä†ä~äSäsäãlç™äZäuä|åHäåŸåQåuä˜åKådåxäžäŸåUåOå›å|æJåŠåšæ}æDæXçUçIçšäYæŸækçåræyæ„æ‰è\çSçMçNæ çOæ—æ›çCç†è‘çhèuç…è|ç’è‰çjç‹èZèCèOèdèsæR·„·wøFøSødøcøù…ûRøzø|úƒøúvøŽø û[ùPûZù]ùOú‘ùYù^ù‘ùgùlù‡ù–ù˜ú\úBúFú_úYûWúpúwúú„úú–ûIú˜ûX°X°Oóœåí°’¯{°A°B°D¯Ž¯›°`°a°]°dÄŸ¸]¸MÒdÑžÒcÒMÒ@Òh¿‹°—ÂgÂeÂœÂ˜í™í î@îRîM}îWîhî€î…ïDî”î‹î—îžïAÍAÏlÏŠÍ˜Ï–Ï Ï|ÍÏuÎ‡Ï“ÏXÏ”ÏNÏ\À›ºV¹a»eº`¹~ºjºD»Xº„ººˆ»f»[ÅœÆA‹–Áu¶i¼c¼g¼Rðf¿{ûŸÚŽõµá‰á‡ûzÜOÛ„Û•ÜVÜEÛ‹Ü]ÜQÜWÜUÜbÛ˜ÜXÜkÜgÓxÓzìnìZìVì\ýZýeý_ýfýbýlýrýpý}üwüxüƒëh×‡èŽçYôœô™õEõGöT÷|·dõV÷cõTõqõ^õnõb÷qõoõœ÷\õ†÷~ö–öžöˆöœõ…õõŒöaõ›öNöOöEöHöKöAöFöTõ öXõ™÷aöl÷{öqövömöcö…üö’ööŠöŽö˜÷B÷Lö öš÷I÷Z÷X÷V÷kí^íXídíxúXótóyóxô|ôuð‹ðôWütüoýBýO‡¾„}„ãìaì^†ß†¤‡Ó‡c‰¡‰³‰Ï‰Ú‹½ŒÚŽGŽAŽMôF§‘€’é“Í•ª•î™„—®™xœtðœÛIøœÝUËªž­m¬„¬š­c­\¯”²”³´o´™¶B·vºš»U¼‡¼Œ¼Ÿ¼…½x½”¾x¾T¿Z¾€¿\ÂPÂEÄsÅNËRÌEÏQÐDÑ‹ÒUÑ‚ÒwÒÓC×„Ó•ÔKÔwÔv×pÕšØrØ’ÚFÙkÙˆÙšÚXÜJÜÝcÝˆÝœáwîÒâlåâ]ãoä€åEã‡ã”äoåWå_èeå}å–ætænè’ægçaçBèGènèé\êGêAêTêFêXíhîcŸâî_îeî„Àhï^ïQï\ï_ïdï}ï€ï˜ïðEðFðLðKðRðkðvðoñ_ñ—óQòHñŸòRò“òjóLô€ô‡ôôŸõOõWõNõQõwõjöfõ`÷dõ~õzõ—õšöYös÷lö[ö“ögöe÷Fö„ö÷@÷s÷h÷gøOúIø{øoøŠø’ù@ùMúAùkùˆùtú‰ùŸù”úgúOúVúWú^úsûDûLûUüsü{ý[ý]º´óa—UÆˆ¬˜Ø‚²[…È„x‰„Å_ƒ´Žr°YœÊ²GÊ|ÆSÌY“{sÃâ üN";
    private static byte[][][]   s2tMap        = new byte[256][256][2];
    private static byte[][][]   t2sMap        = new byte[256][256][2];

    /**
     *³õÊ¼»¯£¬µ÷ÈëGBK¼òÌå¶Ô·±ÌåµÄÂë±í
     */
    static {
        try {
            { //³õÊ¼»¯·±Ìå×ª¼òÌå±í

                byte[] buf1 = GBK_T2S_S_MAP.getBytes("GBK");
                byte[] buf2 = GBK_T2S_T_MAP.getBytes("GBK");

                int    len = Math.min(buf1.length, buf2.length);

                for (int i = 0; i < len; i += 2) {
                    t2sMap[buf2[i] & 0xFF][buf2[i + 1] & 0xFF][0]     = buf1[i];
                    t2sMap[buf2[i] & 0xFF][buf2[i + 1] & 0xFF][1]     = buf1[i + 1];
                }
            }

            { //³õÊ¼»¯¼òÌå×ª·±Ìå±í

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
     * Í¨¹ý¼òÌåÂëµÃµ½·±ÌåÂë
     * @param hbyte
     * @param lbyte
     * @return
     */
    public static byte[] getS2TValue(byte hbyte, byte lbyte) {
        return s2tMap[hbyte & 0xFF][lbyte & 0xFF];
    }

    /**
     * Í¨¹ý·±ÌåÂëµÃµ½¼òÌåÂë
     * @param hbyte
     * @param lbyte
     * @return
     */
    public static byte[] getT2SValue(byte hbyte, byte lbyte) {
        return t2sMap[hbyte & 0xFF][lbyte & 0xFF];
    }

    /**
     * ´Ó¼òÌå×ª»»µ½·±Ìå
     * @param str
     * @return
     */
    public static String covGBKS2T(String str) {
        return covGBK(str, 0);
    }

    /**
     * ´Ó·±Ìå×ª»»³É¼òÌå
     * @param str
     * @return
     */
    public static String covGBKT2S(String str) {
        return covGBK(str, 1);
    }

    /**
     * ´Ó·±Ìå×ª»»³É¼òÌå »ò ´Ó¼òÌå×ª»»µ½·±Ìå
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

                //GBK·¶Î§8140-FEFE
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
     * Èë¿Ú
     * @param args
     */
    public static void main(String[] args) {

    }

    /**
     * ½«×Ö½ÚÁ÷´òÓ¡³öÀ´
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
     * ÅÐ¶ÏÒ»¸ö
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
