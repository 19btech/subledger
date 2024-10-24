package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.entity.AccountTypes;
import org.springframework.batch.item.ItemProcessor;

public class AccountTypeItemProcessor implements ItemProcessor<AccountTypes,AccountTypes> {
    @Override
    public AccountTypes process(AccountTypes item) throws Exception {
        final AccountTypes accountType = new AccountTypes();
        accountType.setAccountType(item.getAccountType());
        accountType.setAccountSubType(item.getAccountSubType());
        return accountType;
    }

}
