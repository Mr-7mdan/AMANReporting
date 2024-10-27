# from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, Date
# from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy.orm import sessionmaker

# Base = declarative_base()

# class ATMBalance(Base):
#     __tablename__ = 'ATMBalances'

#     DetailsID = Column(Integer, primary_key=True)
#     Date = Column(Date)
#     RecordDateEntry = Column(DateTime)
#     OrderUID = Column(String)
#     ClientID = Column(Integer)
#     Client = Column(String)
#     AtmID = Column(Integer)
#     ATM_Name = Column(String)
#     CurrencyID = Column(Integer)
#     Currency = Column(String)
#     OrderDetailsID = Column(Integer)
#     CategoryID = Column(Integer)
#     Deno = Column(String)
#     CassetteTypeID = Column(Integer)
#     CassettePaper = Column(Integer)
#     RejectPaper = Column(Integer)
#     RemainingPaper = Column(Integer)
#     DispensedPaper = Column(Integer)
#     NewPaper = Column(Integer)
#     New_Amount = Column(Float)
#     TotalPaper = Column(Integer)
#     Service_Start_Time = Column(DateTime)
#     Service_End_Time = Column(DateTime)
#     ActualDueTime = Column(DateTime)
#     DueTime = Column(DateTime)
#     LastWithdrawalTime = Column(DateTime)
#     MessionTime = Column(DateTime)
#     StatusName = Column(String)
#     VisitTypeID = Column(Integer)
#     VisitType = Column(String)
#     Preload_Remaining = Column(Integer)
#     Preload_Dispensed = Column(Integer)

# # Create engine and session
# engine = create_engine('sqlite:///atm_forecasting.db')
# Base.metadata.create_all(engine)
# Session = sessionmaker(bind=engine)
