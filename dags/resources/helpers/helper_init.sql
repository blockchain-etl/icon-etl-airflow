/* INIT HELPER TABLES */

CREATE TABLE exchange_wallets( address varchar, exchange varchar );

INSERT
	INTO
	exchange_wallets (address, exchange)
VALUES ('hx1729b35b690d51e9944b2e94075acff986ea0675', 'binance_cold1'),
('hx99cc8eb746de5885f5e5992f084779fc0c2c135b', 'binance_cold2'),
('hx9f0c84a113881f0617172df6fc61a8278eb540f5', 'binance_cold3'),
('hxc4193cda4a75526bf50896ec242d6713bb6b02a3', 'binance_hot'),
('hx307c01535bfd1fb86b3b309925ae6970680eb30d', 'velic_hot'),
('hxff1c8ebad1a3ce1ac192abe49013e75db49057f8', 'velic_stave'),
('hx14ea4bca6f205fecf677ac158296b7f352609871', 'latoken'),
('hx3881f2ba4e3265a11cf61dd68a571083c7c7e6a5', 'coinex'),
('hxd9fb974459fe46eb9d5a7c438f17ae6e75c0f2d1', 'huobi'),
('hx68646780e14ee9097085f7280ab137c3633b4b5f', 'kraken_hot');

CREATE TABLE score_addresses ( address varchar, "name" varchar);

INSERT INTO score_addresses (address,"name") VALUES
	 ('cx9ab3078e72c8d9017194d17b34b1a47b661945ca','AC3'),
	 ('cx502c47463314f01e84b1b203c315180501eb2481','ACT'),
	 ('cx274424e7f501ff60b13fa624ce70e03e7a2b0c12','AdventureGame'),
	 ('cxa3512fb64b8808ca07a8469e391b33d7243153b5','Andrew Token'),
	 ('cx1a59d517ede7ac1f7e964f8e003fb2d67e2f6ac0','Be a rich'),
	 ('cxfdc37ff554fd0a7fae0073984d5e0d89739a7bb6','Beas Coin'),
	 ('cx6139eb8b6c78fc51b3de503aa367e5e5fb06d5c3','BiggerAndBigger'),
	 ('cxbdda1241313c0113f8ebf4b974239b145558513a','BLOC8'),
	 ('cxa4524257b3511fb9574009785c1f1e73cf4097e7','Bookmarker'),
	 ('cxe1c39cac4b1976ae81c3fc0a4bd654f884c775bf','BOOKSON'),
	 ('cxf537f85007f26415a82e9b41ea77fa6ec2e3e4cd','broof'),
	 ('cx69bcdf1753472c1444188ec3f5188657e30c8322','broof - we prove, verify and love you.'),
	 ('cx23c52f0fd6b126af25ddbe65f983f9518e9f1a73','broof - we prove, verify and love you.'),
	 ('cx5931aa6e4870efea295850500b80166328cfa2b4','CJ Cooperation'),
	 ('cxa0669cd3556ae5f5232a899a69a5919152463885','Cockroach'),
	 ('cx188a591a29aae1c45f8e63eab2266e0de398bbdb','CommentCeleb'),
	 ('cxe5b011aa1c77e212b01c4f8755d03ccdc3183239','Coordination'),
	 ('cxbd5e78a4a1631fd12eb8742799cd11425b0bd615','DAOcheckers'),
	 ('cx903cad3bcf74637a0b7ceed7383ed0cada20a635','DAOcheckers'),
	 ('cx299d88908ab371d586c8dfe0ed42899a899e6e5b','DAOlevels'),
	 ('cxb757a72f44dc156283847957290beff14681a522','DestinyGame'),
	 ('cx155c7aaa9de2add77d322713e31dee4ac43094e7','DiceCoinX'),
	 ('cx17eb6014740f2ae1d547df76fa593f988986661b','DiceCoinX'),
	 ('cx2b807cc77ed974a1cecee72e716a68e8407957e7','Distinction_Numbers'),
	 ('cx5daa5cdb3a719e16c50a0457e54f6913511575b4','Distinction_Numbers'),
	 ('cx4b354f9455bd6ece74a7d2999a5ce3e6fbcb479c','DockGame'),
	 ('cx078f6da833c24301c5a3bf0382119114d55dea36','DotB'),
	 ('cxba7a8271d85ed673d27574a30e3261e147902e92','ELGUAPO COIN'),
	 ('cxc50036d08b5ffa74f3c2ab435e149513fbc6a8b1','FavoriteTrip'),
	 ('cxfd0c534e8aa731aff14740eb32ffad47b5d848c4','FoodieShareList'),
	 ('cx9d6048d76c093dfc6e2aa5035f61a5cbdb787250','GambleThreeCups'),
	 ('cx9da444c74642c5c6f4087080213d0ba5870a0547','GirlsShoppingMall'),
	 ('cx51b37fe4890df4125918f4ee33885ae0a9ce52b0','Go to the sky'),
	 ('cx0558addd1c767630235f2371cf75986f7788cd1e','HigherSalary'),
	 ('cx84e5e81fc7cd5357025e29bb70d342e15d8beb44','HobbyLobbyGood'),
	 ('cx82e9075445764ef32f772d11f5cb08dae71d463b','ICON Talent Donation'),
	 ('cxfccb5d387c652a8d2a164200da658e7d832591d1','ICON Talent Donation'),
	 ('cxc1d5ca6d7daede82e59b616ea6908807d7bea3b8','ICON Wish Board'),
	 ('cx744e0710cc30433e4563eca51f2ab341dda76c93','ICON World'),
	 ('cxd9858f1c48df0e9b1237c2a757d468b41bd94fc1','ICONBET'),
	 ('cxd47f7d943ad76a0403210501dab03d4daf1f6864','ICONbet DAOblackjack'),
	 ('cx1c06cf597921e343dfca2883f699265fbec4d578','ICONbet Lottery'),
	 ('cxeebe72b26fc055cf0a48b8be3961dea3bbdb0541','ICONbet Promotion'),
	 ('cx81fe20ac9a8ed7387b8d17be878c1d0ccb01aabf','IconGameAlliance'),
	 ('cx67d48a0f1e91d1e1e85a830a80fa396fa446a13d','ICONNetBet'),
	 ('cx6f824b1ccc08c13f626b8fc888d8214632cba28a','ICONSwap'),
	 ('cxce68b59d5facc473ca4721a5be473ee569d0f576','ICONSwapTreasury'),
	 ('cx7525d37107513415796e81326fc26a477721ab8c','ICONVIET MEGALOOP'),
	 ('cxb6eb15f06653d40e2259b7f41d240e597cffb8cb','icon_delta_token'),
	 ('cx862833853e07ab4f808a1532efb48aafbeaf5d81','Insight Token'),
	 ('cx46d5add490b429f7f5a54b99f5998a307c940bc2','InstaFellas'),
	 ('cx993810b4523ab6b1658925d8d6c234f286adbdba','IWinToken'),
	 ('cxd556016640336a4f8d3bd1beb800b60c0041532e','KoploToken'),
	 ('cx54bdc94049041acaedae695ec703aba3fd4c8dcc','KRWb Token'),
	 ('cxbd31ecb07aca9f6f65e00cbcb94189eb209c5148','KRWb Token'),
	 ('cx6f5c6f7ce087a9370cef5b16e2493d5a57f94bcf','Laptop_Classification'),
	 ('cx637183ae36796568d265e4bf855a7c762acc5ae7','LetterToFriend'),
	 ('cx11ffd85e01fa2a9093c929b9bf1d20530efc8ea3','LiteBlocklords'),
	 ('cx2137642d0bf1926fbe23a3688d042a0f34bc2b9a','LogisticsX'),
	 ('cx4db4b2c91b8986d6991b9649e86edea17b940bce','LotteryPrediction'),
	 ('cx2916b5c6af83e8199500ed0ff758545bbc4b58ef','Love Icon'),
	 ('cx682792a2f91a3cfc88eec357d05271d55263c3fd','Major_Country_Continent'),
	 ('cxf9148db4f8ec78823a50cb06c4fed83660af38d0','MECA Coin'),
	 ('cxb62bef3b1a6e2494a3cf9a6c674b45153157af2a','Membership_Management'),
	 ('cx7ed188dd5f3bde7c302d3856c745712e9f81098b','MessageSoldier'),
	 ('cx9f63c861272daadf76c12afa7d1b27ffd46fe00c','MineGame'),
	 ('cxc28373289f025bb139e46ceee71e0399b8e01440','Mining IKON'),
	 ('cx490667db346c02fcce0f7e4bd24e4f52269dfffb','MsgtoIcon'),
	 ('cx1485a8d99c0c670cc52429dc2e999830427d9941','MYID'),
	 ('cx195e525678d51e14bc801182256d322f740fb38a','One Password'),
	 ('cx8c43c6980d57aee6a0c69fb3a2f3c30fb0801b6d','PATRICK'),
	 ('cx321780e9a8fddbac9866e98752d3cc79bab7d63e','ProductVote'),
	 ('cxea312eb662a97659d1ccf44381eeb2c943161bb7','P_Rep Token'),
	 ('cx429731644462ebcfd22185df38727273f16f9b87','Somesing Exchange'),
	 ('cx0be614dec0c0bfd94aee01a5af579b6c58f7f386','SpeakyToGovernance'),
	 ('cxbd7f41fb563f3eac0404bfb47a33c530376dc1be','SpeakyToToken'),
	 ('cx08eb683a53d6f244d538a45f0cc584825e2cb985','SportsToken'),
	 ('cx3ec2814520c0096715159b8fc55fa1f385be038c','SportToken'),
	 ('cx5d88e057f895e9f6d7440f6294707df4d022fff3','Tap01d'),
	 ('cx021c67057bbfd77c2de5636eab15ff228e042629','Tap02d'),
	 ('cxc0b5b52c9f8b4251a47e91dda3bd61e5512cd782','TapToken'),
	 ('cxbf638bac4c5f7905141f25a3add16ba5544a0417','Test Token'),
	 ('cx799bf4408212fb670665a7eb519fe5bb0ee3549c','The diet'),
	 ('cx8c392a353ee556406684f1508fb1cace790745c5','TipTAP'),
	 ('cxc39d322cd9af864f0edb0bfe9635322893e07c5b','Todays Idol'),
	 ('cxbcd70349b0853e484b99138cb288bb08435e7b88','TodaysQuote'),
	 ('cxefaa21e34a3a1abf97369b5beef84524f52d88a8','Velic Authority'),
	 ('cx19a23e850bf736387cd90d0b6e88ce9af76a8d41','Velic Token'),
	 ('cxbc264e6279ec971f11ebe3939fc88d05b243eba7','VELICX'),
	 ('cx921205acb7c51e16d5b7cbc37539a4357d929d20','weBloc'),
	 ('cx3afc8f780c3b56a7417896f3d9e0e3c051840b30','WhereInUsa'),
	 ('cx0b6d085c4f42ca85da612a8cc02a519241587e14','WideNEQ'),
	 ('cxf05884546c2e914db0ba1726f715982651c2b497','WithU'),
	 ('cxc248ee72f58f7ec0e9a382379d67399f45b596c7','WITHU'),
	 ('cx57795da3844a5004d5196312167f08b31d75a9a3','ZperToken'),
	 ('cx8968ef96602f08ac0bdcc0d4878f07f8527fce45','ZperToken'),
	 ('cx0000000000000000000000000000000000000000','ICON Network Interaction');

/* INIT MATERIALIZED VIEWS */

CREATE MATERIALIZED VIEW reduced_trans AS (
SELECT
	to_address,
	from_address,
	value,
	to_timestamp(LEFT(timestamp::TEXT, 10)::int8) AS timestamp,
	data_type,
	DATA
FROM
	transactions ) CREATE MATERIALIZED VIEW current_period AS (
SELECT
	*
FROM
	reduced_trans
WHERE
	timestamp BETWEEN current_date - integer '30' AND current_date
	AND from_address LIKE 'hx%' );

CREATE MATERIALIZED VIEW previous_period AS (
SELECT
	*
FROM
	reduced_trans
WHERE
	timestamp BETWEEN current_date - integer '60' AND current_date - integer '31'
	AND from_address LIKE 'hx%' );