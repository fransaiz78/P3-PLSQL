--Autores: Francisco Saiz Guemes - Mario Santamaria Arias

DROP TABLE Elementos cascade constraint;
DROP TABLE Moleculas cascade constraint;
DROP TABLE Composicion cascade constraint;
DROP SEQUENCE seq_molId;

CREATE TABLE Elementos (
  simbolo varchar(3),
  nombre varchar(20) UNIQUE NOT NULL,
  pesoAtomico integer NOT NULL,
  PRIMARY KEY(simbolo)
);

CREATE TABLE Moleculas (
  id integer,
  nombre varchar(20) UNIQUE NOT NULL,
  pesoMolecular integer NOT NULL,
  formula varchar(20) UNIQUE NOT NULL,
  PRIMARY KEY(id)
);

CREATE TABLE Composicion (
  simbolo varchar(3),
  idMolecula integer,
  nroAtomos integer NOT NULL,  
  PRIMARY KEY(simbolo, idMolecula),
  FOREIGN KEY(simbolo) REFERENCES Elementos(simbolo),
  FOREIGN KEY(IdMolecula) REFERENCES Moleculas(id)
);

CREATE SEQUENCE seq_molId; 

insert into Elementos(simbolo, nombre, pesoAtomico) values ('H','Hidrogeno', 1);
insert into Elementos(simbolo, nombre, pesoAtomico) values('O','Oxigeno', 18);

--insert into Moleculas(id, nombre, pesoMolecular, formula) values(seq_molId.nextval, 'Agua', 20, 'H2O');

--insert into Composicion(simbolo, idMolecula, nroAtomos) values('H', seq_molId.currval, 2);
--insert into Composicion(simbolo, idMolecula, nroAtomos) values('O', seq_molId.currval, 1);

commit;

--Creamos los procedimientos.
CREATE OR REPLACE TYPE NESTED_TYPE IS TABLE OF VARCHAR2(20);
/
------------------------------------------------------------------------------------------------------------------------------
--- BORRAR POR ID
------------------------------------------------------------------------------------------------------------------------------
create or replace procedure borrarMoleculaId(id_mol INTEGER) is
  
  ID_INEXISTENTE  exception;
  PRAGMA EXCEPTION_INIT(ID_INEXISTENTE, -20000);

begin
  
  --Comprobamos cuantas filas borra
  delete from Composicion
  where idMolecula=id_mol;
  
  --Si no se ha borrado ninguna fila es que el id no existe.
  if(sql%ROWCOUNT)=0 then
    raise_application_error(-20000, 'Id inexistente.');
  else
    delete from Moleculas
    where id=id_mol;
  end if;
  
  commit;
exception
  when OTHERS then 
    rollback;
    raise_application_error(SQLCODE, 'No existe el correspondiente Id.');
end;
/

------------------------------------------------------------------------------------------------------------------------------
--- BORRAR POR NOMBRE
------------------------------------------------------------------------------------------------------------------------------
create or replace procedure borrarMoleculaNombre(nombre_mol varchar) is
  
  v_id_mol Integer;
  
  NOMBRE_MOLECULA_NO_EXISTE  exception;
  PRAGMA EXCEPTION_INIT(NOMBRE_MOLECULA_NO_EXISTE, -20000);

begin
  
  --Comprobamos si existe el nombre, sino lanza excepcion.
  select id into v_id_mol
  from Moleculas
  where nombre=nombre_mol;
  
  --Existe nombre, con el id borramos en composicion y Moleculas
  delete from Composicion
  where idMolecula=v_id_mol;
  
  delete from Moleculas
  where id=v_id_mol;
  
  commit;
exception
  when OTHERS then
    rollback;
    raise_application_error(-20000, 'Nombre de la molecula no existe.');
end;
/

------------------------------------------------------------------------------------------------------------------------------
--- ACTUALIZAR POR ID
------------------------------------------------------------------------------------------------------------------------------
create or replace procedure actualizarMoleculaId(p_id Integer, p_simbolo varchar, p_nro Integer) is
  v_simbolo varchar(20);
  v_id_mol Integer;
  v_nroAtomos Integer;
  --Tipo de la fila del cursor.
  v_filaCursor Composicion%ROWTYPE;
  
  v_pesoMolecularTotal Integer:=0;
  v_formulaFinal varchar(20):='';
  
  cursor c_nroSimbolo is 
    SELECT * FROM Composicion WHERE idMolecula=p_id;
  
  cursor c_joinElemComp is
    SELECT elementos.simbolo, elementos.nombre, elementos.pesoatomico, composicion.nroatomos FROM Elementos inner join Composicion ON Elementos.simbolo = composicion.simbolo and composicion.idMolecula=p_id;
  
  v_j_simbolo Elementos.simbolo%TYPE;
  v_j_nombre Elementos.nombre%TYPE;
  v_j_pesoAtomico Elementos.pesoatomico%TYPE;
  v_j_nroAtomos Composicion.nroatomos%TYPE;
  
  FORMULA_YA_EXISTENTE  exception;
  ID_INEXISTENTE exception;
  MOLECULA_NO_CONTIENE_SIMBOLO  exception;
  
  PRAGMA EXCEPTION_INIT(FORMULA_YA_EXISTENTE, -20000);
  PRAGMA EXCEPTION_INIT(MOLECULA_NO_CONTIENE_SIMBOLO, -20001);
  PRAGMA EXCEPTION_INIT(ID_INEXISTENTE, -20002);

begin
  
  begin
    --Comprobamos que existe una molecula con ese id. Si no, lanza excepcion (no_data_found)
    SELECT id into v_id_mol FROM Moleculas WHERE id=p_id;
  
    --Comprobamos que el nroAtomico y el simbolo no son los mismos que los existentes con un cursor.
    open c_nroSimbolo; 
    loop
      fetch c_nroSimbolo into v_filaCursor;
      exit when c_nroSimbolo%notfound;
      --Comprobamos mediante el simbolo si tienen el mismo nroAtomico
      if (v_filaCursor.simbolo = p_simbolo and v_filaCursor.nroAtomos = p_nro) then
        RAISE FORMULA_YA_EXISTENTE;
      end if;
    end loop;
    close c_nroSimbolo;
    
    
    --Una vez que sabemos que la formula no existia, actualizamos en Composicion.
    UPDATE Composicion set nroAtomos=p_nro WHERE idMolecula=p_id and simbolo=p_simbolo;
    --Comprobamos si el simbolo existia o no existia
    if(sql%ROWCOUNT)=0 then
      raise MOLECULA_NO_CONTIENE_SIMBOLO;
    end if;
    
    
    --Calculamos el pesoMolecular y concatenamos la formula.
    for v_elem in c_joinElemComp loop
      v_pesoMolecularTotal := v_pesomoleculartotal + (v_elem.pesoAtomico*v_elem.nroAtomos);
      v_formulaFinal := v_formulaFinal || v_elem.simbolo;
      if (v_elem.nroAtomos>1) then
        v_formulaFinal := v_formulaFinal || v_elem.nroAtomos;
      end if;
    end loop;
  
    --Realizamos el update en la tabla Moleculas
    UPDATE Moleculas set pesoMolecular=v_pesoMolecularTotal, formula=v_formulafinal WHERE id=p_id;
    
    commit;
  exception
    when no_data_found then
      raise_application_error(-20002, 'No existe molecula con ese id.' );
    when MOLECULA_NO_CONTIENE_SIMBOLO then
      raise_application_error(-20001, 'Molecula no contiene simbolo.' );
    when FORMULA_YA_EXISTENTE then
      raise_application_error(-20000, 'Molecula con formula existente.' );
  end;

exception
  when OTHERS then 
    rollback;
    raise_application_error( SQLCODE, SQLERRM );

end;
/
------------------------------------------------------------------------------------------------------------------------------
--- ACTUALIZAR POR NOMBRE
------------------------------------------------------------------------------------------------------------------------------
create or replace procedure actualizarMoleculaNombre(p_nombre varchar, p_simbolo varchar, p_nro Integer) is
  v_simbolo varchar(20);
  v_id_mol Integer;
  v_nombre_mol varchar(20);
  v_nroAtomos Integer;
  --Tipo de la fila del cursor.
  v_filaCursor Composicion%ROWTYPE;
  
  v_pesoMolecularTotal Integer:=0;
  v_formulaFinal varchar(20):='';
  
  cursor c_nroSimbolo is 
    SELECT * FROM Composicion WHERE idMolecula=v_id_mol;
  
  cursor c_joinElemComp is
    SELECT elementos.simbolo, elementos.nombre, elementos.pesoatomico, composicion.nroatomos FROM Elementos inner join Composicion ON Elementos.simbolo = composicion.simbolo and composicion.idMolecula=v_id_mol;
  
  v_j_simbolo Elementos.simbolo%TYPE;
  v_j_nombre Elementos.nombre%TYPE;
  v_j_pesoAtomico Elementos.pesoatomico%TYPE;
  v_j_nroAtomos Composicion.nroatomos%TYPE;
  
  FORMULA_YA_EXISTENTE  exception;
  NOMBRE_INEXISTENTE exception;
  MOLECULA_NO_CONTIENE_SIMBOLO  exception;
  
  PRAGMA EXCEPTION_INIT(FORMULA_YA_EXISTENTE, -20000);
  PRAGMA EXCEPTION_INIT(MOLECULA_NO_CONTIENE_SIMBOLO, -20001);
  PRAGMA EXCEPTION_INIT(NOMBRE_INEXISTENTE, -20002);

begin
  
  begin  
    --sacamos el id asociado a ese nombre 
    select id into v_id_mol FROM Moleculas WHERE nombre=p_nombre;
  
    --Comprobamos que el nroAtomico y el simbolo no son los mismos que los existentes con un cursor.
    open c_nroSimbolo; 
    loop
      fetch c_nroSimbolo into v_filaCursor;
      exit when c_nroSimbolo%notfound;
      --Comprobamos mediante el simbolo si tienen el mismo nroAtomico
      if (v_filaCursor.simbolo = p_simbolo and v_filaCursor.nroAtomos = p_nro) then
        RAISE FORMULA_YA_EXISTENTE;
      end if;
    end loop;
    close c_nroSimbolo;
    
    --Una vez que sabemos que la formula no existia, actualizamos en Composicion.
    UPDATE Composicion set nroAtomos=p_nro WHERE idMolecula=v_id_mol and simbolo=p_simbolo;
    --Comprobamos si el simbolo existia o no existia
    if(sql%ROWCOUNT)=0 then
      raise MOLECULA_NO_CONTIENE_SIMBOLO;
    end if;
    
    --Calculamos el pesoMolecular y concatenamos la formula.
    for v_elem in c_joinElemComp loop
      v_pesoMolecularTotal := v_pesomoleculartotal + (v_elem.pesoAtomico*v_elem.nroAtomos);
      v_formulaFinal := v_formulaFinal || v_elem.simbolo;
      if (v_elem.nroAtomos>1) then
        v_formulaFinal := v_formulaFinal || v_elem.nroAtomos;
      end if;
    end loop;
  
    --Realizamos el update en la tabla Moleculas
    UPDATE Moleculas set pesoMolecular=v_pesoMolecularTotal, formula=v_formulafinal WHERE id=v_id_mol;
    
    commit;
  exception
    when no_data_found then
      raise_application_error(-20002, 'No existe molecula con ese nombre.' );
    when MOLECULA_NO_CONTIENE_SIMBOLO then
      raise_application_error(-20001, 'Molecula no contiene simbolo.' );
    when FORMULA_YA_EXISTENTE then
      raise_application_error(-20000, 'Molecula con formula existente.' );
  end;

exception
  when OTHERS then
    rollback;
    raise_application_error(SQLCODE, SQLERRM );
end;
/

------------------------------------------------------------------------------------------------------------------------------
--- INSERTAR
------------------------------------------------------------------------------------------------------------------------------

create or replace procedure insertarMolecula(p_nombre varchar, p_simbolos in out NESTED_TYPE, p_nros in out NESTED_TYPE) is
  
  v_nombre_mol varchar(20);
  v_formulaFinal varchar(20):='';
  v_pesoMolecularTotal Integer:=0;
  v_pesoMolecular Integer;
  v_consulta_formula Moleculas.formula%TYPE;
  
  v_simb_aux varchar(3);
  v_num_aux integer;
  
  TAMAÑOS_INADECUADOS  exception;
  NOMBRE_YA_EXISTENTE exception;
  NO_EXISTE_ATOMO  exception;
  --FORMULA_YA_EXISTENTE exception; Usamos la DUP_VAL_ON_INDEX
  
  PRAGMA EXCEPTION_INIT(TAMAÑOS_INADECUADOS, -20000);
  PRAGMA EXCEPTION_INIT(NOMBRE_YA_EXISTENTE, -20001);
  PRAGMA EXCEPTION_INIT(NO_EXISTE_ATOMO, -20002);
  --PRAGMA EXCEPTION_INIT(FORMULA_YA_EXISTENTE, -20003); Usamos la DUP_VAL_ON_INDEX
  
begin

  begin
    if(p_simbolos.count != p_nros.count) then
      raise TAMAÑOS_INADECUADOS;
    end if;
  
    --Ordenamos los arrays a traves del algoritmo de la burbuja.
    FOR i IN p_simbolos.FIRST .. p_simbolos.LAST LOOP
      FOR j IN p_simbolos.FIRST .. p_simbolos.LAST-1 LOOP
        if( p_simbolos(j) > p_simbolos(j+1) ) then
          --Cambiamos simbolos
          v_simb_aux:=p_simbolos(j);
          p_simbolos(j):=p_simbolos(j+1);
          p_simbolos(j+1):=v_simb_aux;
          --Cambiamos nros
          v_num_aux:=p_nros(j);
          p_nros(j):=p_nros(j+1);
          p_nros(j+1):=v_num_aux;
        end if;
      END LOOP;
    END LOOP;
    
    --Comprobamos que el nombre de la molecula a insertar existe
    SELECT count(nombre) into v_nombre_mol FROM Moleculas WHERE nombre=p_nombre;
    if(v_nombre_mol > 0) then
      raise NOMBRE_YA_EXISTENTE;
    end if;
    
    --Calcular la formula y calcular el peso molecular.
    FOR i IN p_simbolos.FIRST .. p_simbolos.LAST
    LOOP
      --concatenamos la formula cada iteracion.
      v_formulaFinal := v_formulaFinal || p_simbolos(i);
      if(p_nros(i)>1) then
        v_formulaFinal := v_formulaFinal || p_nros(i);
      end if;
      
      --calculamos el pesoMolecular total.
      select pesoAtomico into v_pesoMolecular from Elementos Where simbolo=p_simbolos(i);
      v_pesoMolecularTotal := v_pesoMolecularTotal + v_pesoMolecular*p_nros(i);
    
    END LOOP;
    
    --Insertamos en Moleculas.
    INSERT INTO Moleculas(id, nombre, pesoMolecular, formula) values(seq_molId.nextval, p_nombre, v_pesoMolecularTotal, v_formulaFinal);
    
    --Insertamos en composicion.
    FOR i IN p_simbolos.FIRST .. p_simbolos.LAST
    LOOP
      INSERT INTO Composicion(simbolo, idMolecula, nroAtomos) values(p_simbolos(i), seq_molId.currval, p_nros(i));
    END LOOP;
    
    commit;
    exception
      when TAMAÑOS_INADECUADOS then
        raise_application_error(-20000, 'Tamaño de los arrays inadecuado.');
      when NOMBRE_YA_EXISTENTE then
        raise_application_error(-20001, 'Nombre de la molecula ya existente.');
      when DUP_VAL_ON_INDEX then
        raise_application_error(-20003, 'Ya existe molecula con esa formula.');
      when NO_DATA_FOUND then
        raise_application_error(-20002, 'No existe atomo.');
    end;
    
exception
  
  when OTHERS then 
    rollback;
    raise_application_error( SQLCODE, SQLERRM );

end;
/

--Comentaremos y descomentaremos en funcion de su ejecucion desde java o developer.
exit;

--Pruebas de que los procedimientos funcionan correctamente.

--Para las pruebas en Java hemos considerado oportuno empezar las pruebas con las tablas Moleculas y Composicion vacías, pero
--para probarlo desde aqui, deberiamos descomentar los inserts correspondientes a las tablas Moleculas y Composicion 
--situados encima de la implementacion de los procedimientos.

/*
--Poner el serveroutput a on
set serveroutput on

declare
  v1 Nested_type;
  v2 Nested_type;
begin

  dbms_output.put_line('Pruebas:');
  
  v1:=Nested_type();
  v1.extend(2);
  
  v1(1):='O';
  v1(2):='H';

  v2:=Nested_type();
  v2.extend(2);
  
  v2(1):=1;
  v2(2):=2;
  
  --borrarMoleculaId(1);
  --borrarMoleculaNombre('Agua');
  --actualizarMoleculaId(1,'H',4);
  --actualizarMoleculaNombre('Agua','O',7);
  --insertarMolecula('AguaOxigenada',v1,v2);
  
end;
/

exit;
*/

/*
select * FROM Moleculas
select * FROM Composicion
select * FROM Elementos
*/


